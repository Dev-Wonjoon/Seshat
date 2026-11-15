package com.seshat.repair;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.util.Optional;

@Repository
public class JsonRepairRepository {

    private static final String REGISTER_SQL = """
            INSERT INTO json_repair_job (run_id, source_id, event_id)
            VALUES (?, ?, ?)
            ON CONFLICT (run_id, source_id, event_id) DO NOTHING
            """;

    private static final String CLAIM_SQL = """
            UPDATE json_repair_job
            SET state = 'PROCESSING',
                attempt_count = attempt_count + 1,
                claim_until_ms = ?,
                provider = COALESCE(provider, ?),
                target_id = COALESCE(target_id, ?),
                model = COALESCE(model, ?),
                prompt_version = COALESCE(prompt_version, ?)
            WHERE id = (
                SELECT job.id
                FROM json_repair_job job
                JOIN log_queue original USING (run_id, source_id, event_id)
                WHERE job.state IN ('PENDING', 'RETRY_WAIT')
                  AND job.next_attempt_at_ms <= ?
                  AND original.result_json IS NOT NULL
                ORDER BY job.id
                LIMIT 1
            )
            RETURNING id, attempt_count, provider, target_id,
                      model, prompt_version,
                      (SELECT result_json FROM log_queue original
                       WHERE original.run_id = json_repair_job.run_id
                         AND original.source_id = json_repair_job.source_id
                         AND original.event_id = json_repair_job.event_id) AS original_result_json
            """;

    private final JdbcTemplate jdbc;

    public JsonRepairRepository(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    public void register(String runId, String sourceId, String eventId) {
        jdbc.update(REGISTER_SQL, runId, sourceId, eventId);
    }

    @Transactional
    public Optional<Job> claim(
            long now,
            long claimUntil,
            int maxAttempts,
            String provider,
            JsonRepairClient.Profile profile
    ) {
        recoverExpiredClaims(now, maxAttempts);

        var jobs = jdbc.query(CLAIM_SQL, (rs, index) -> new Job(
                rs.getLong("id"),
                rs.getInt("attempt_count"),
                rs.getString("provider"),
                new JsonRepairClient.Profile(
                        rs.getString("target_id"), rs.getString("model"), rs.getString("prompt_version")),
                rs.getString("original_result_json")
        ), claimUntil, provider, profile.targetId(), profile.model(), profile.promptVersion(), now);

        if (jobs.isEmpty()) {
            return Optional.empty();
        }

        Job job = jobs.getFirst();
        jdbc.update("""
                INSERT INTO json_repair_attempt (
                    job_id, attempt_number, started_at_ms, provider, target_id, model, prompt_version
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """, job.id(), job.attempt(), now, job.provider(),
                job.profile().targetId(), job.profile().model(), job.profile().promptVersion());
        return Optional.of(job);
    }

    private void recoverExpiredClaims(long now, int maxAttempts) {
        jdbc.update("""
                UPDATE json_repair_attempt
                SET finished_at_ms = ?, outcome = 'CALL_FAILED', reason = 'INTERRUPTED'
                WHERE finished_at_ms IS NULL
                  AND (job_id, attempt_number) IN (
                      SELECT id, attempt_count FROM json_repair_job
                      WHERE state = 'PROCESSING' AND claim_until_ms <= ?
                  )
                """, now, now);

        jdbc.update("""
                UPDATE json_repair_job
                SET state = CASE WHEN attempt_count < ? THEN 'RETRY_WAIT' ELSE 'CALL_FAILED' END,
                    reason = 'INTERRUPTED', next_attempt_at_ms = ?, claim_until_ms = NULL
                WHERE state = 'PROCESSING' AND claim_until_ms <= ?
                """, maxAttempts, now, now);

        jdbc.update("""
                UPDATE json_repair_job
                SET state = 'CALL_FAILED'
                WHERE state = 'RETRY_WAIT' AND attempt_count >= ?
                """, maxAttempts);
    }

    @Transactional
    public void finish(Job job, Outcome outcome, long now, long nextAttempt) {
        // 회수된 작업의 늦은 응답은 새 시도의 결과를 덮어쓸 수 없다.
        int updated = jdbc.update("""
                UPDATE json_repair_job
                SET state = ?, reason = ?, next_attempt_at_ms = ?, claim_until_ms = NULL
                WHERE id = ? AND state = 'PROCESSING' AND attempt_count = ?
                """, nextAttempt > 0 ? "RETRY_WAIT" : outcome.state(), outcome.reason(),
                nextAttempt, job.id(), job.attempt());
        if (updated == 1) {
            jdbc.update("""
                    UPDATE json_repair_attempt
                    SET finished_at_ms = ?, outcome = ?, reason = ?, model_decision = ?,
                        model_reason = ?, response_text = ?,
                        candidate_json = ?, syntax_valid = ?, values_preserved = ?
                    WHERE job_id = ? AND attempt_number = ?
                    """, now, outcome.state(), outcome.reason(),
                    outcome.response() == null || outcome.response().decision() == null
                            ? null : outcome.response().decision().name(),
                    outcome.response() == null ? null : outcome.response().reason(),
                    outcome.response() == null ? null : outcome.response().responseText(),
                    outcome.response() == null ? null : outcome.response().candidateJson(),
                    outcome.syntaxValid(), outcome.valuesPreserved(),
                    job.id(), job.attempt());
        }
    }

    public record Job(
            long id, int attempt,
            String provider, JsonRepairClient.Profile profile, String originalResult
    ) {
    }

    public record Outcome(
            String state, String reason, RepairCandidate response,
            Boolean syntaxValid, Boolean valuesPreserved
    ) {
        static Outcome withoutCandidate(String state, String reason) {
            return new Outcome(state, reason, null, null, null);
        }
    }
}
