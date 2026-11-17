package com.seshat.queue;

import com.seshat.ingestion.dto.LogEventRequest;
import com.seshat.ingestion.dto.ParseStatus;
import com.seshat.ingestion.dto.ParsingMode;
import com.seshat.processing.JavaLogProcessor;
import com.seshat.repair.JsonRepairRepository;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionTemplate;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.node.ObjectNode;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

@Service
public class QueueProcessingService {

    private static final String CLAIM_SQL = """
        WITH ready AS MATERIALIZED (
            SELECT id
            FROM log_queue
            WHERE state = 'PENDING'
               OR (state = 'RETRY_WAIT' AND next_attempt_at_ms <= ?)
            ORDER BY id
            LIMIT ?
        ),
        sized AS MATERIALIZED (
            SELECT log_queue.id,
                   coalesce(octet_length(result_json), octet_length(payload_json)) AS bytes
            FROM ready JOIN log_queue USING (id)
        ),
        bounded AS (
            SELECT id, bytes, sum(bytes) OVER (ORDER BY id) AS total_bytes
            FROM sized
        )
        UPDATE log_queue
        SET state = 'PROCESSING',
            processing_started_at_ms = ?,
            attempt_count = attempt_count + 1,
            next_attempt_at_ms = 0
        WHERE id IN (
            SELECT id FROM bounded
            WHERE total_bytes <= ? OR total_bytes = bytes
        )
        RETURNING id, run_id, source_id, event_id, mode,
                  CASE WHEN result_json IS NULL THEN payload_json END AS payload_json,
                  result_json, attempt_count
        """;

    private static final String SAVE_SQL = """
        UPDATE log_queue
        SET result_json = ?
        WHERE id = ? AND state = 'PROCESSING' AND attempt_count = ?
        """;

    private static final String RELEASE_SQL = """
        UPDATE log_queue
        SET state = 'PENDING', processing_started_at_ms = NULL, attempt_count = attempt_count - ?
        WHERE id = ? AND state = 'PROCESSING' AND attempt_count = ?
        """;

    private final JdbcTemplate jdbcTemplate;
    private final JsonMapper mapper;
    private final JavaLogProcessor processor;
    private final TransactionTemplate transactions;
    private final JsonRepairRepository repairJobs;
    private final boolean repairEnabled;

    public QueueProcessingService(
            JdbcTemplate jdbcTemplate,
            JsonMapper mapper,
            JavaLogProcessor processor,
            PlatformTransactionManager transactionManager,
            JsonRepairRepository repairJobs,
            @Value("${seshat.repair.enabled:false}") boolean repairEnabled
    ) {
        this.jdbcTemplate = jdbcTemplate;
        this.mapper = mapper;
        this.processor = processor;
        this.transactions = new TransactionTemplate(transactionManager);
        this.repairJobs = repairJobs;
        this.repairEnabled = repairEnabled;
    }

    @Transactional(propagation = Propagation.NEVER)
    public Optional<PreparedLogEvent> prepareNext() {
        var batch = prepareBatch(1, Integer.MAX_VALUE);
        if (batch.failure() != null) {
            throw batch.failure();
        }
        return batch.events().stream().findFirst();
    }

    @Transactional(propagation = Propagation.NEVER)
    public PreparedBatch prepareBatch(int maxEvents, int maxBytes) {
        if (maxEvents < 1 || maxBytes < 1) {
            throw new IllegalArgumentException("Queue batch limits must be positive.");
        }
        var claimed = claimBatch(maxEvents, maxBytes);
        if (claimed.isEmpty()) {
            return new PreparedBatch(List.of(), null);
        }
        var prepared = new ArrayList<PreparedLogEvent>(claimed.size());
        var updates = new ArrayList<Object[]>(claimed.size());
        RuntimeException preparationFailure = null;
        try {
            for (var queued : claimed) {
                var result = prepareResult(queued);
                var json = mapper.writeValueAsString(result);
                updates.add(new Object[]{json, queued.id(), queued.attemptCount()});
                prepared.add(new PreparedLogEvent(
                        queued.id(), queued.runId(), queued.sourceId(), queued.eventId(),
                        queued.mode(), result, queued.attemptCount()
                ));
            }
        } catch (RuntimeException exception) {
            preparationFailure = exception;
        }

        try {
            var remaining = claimed.subList(prepared.size(), claimed.size());
            transactions.executeWithoutResult(status -> {
                saveResults(prepared, updates);
                release(remaining, 1);
            });
        } catch (RuntimeException exception) {
            try {
                int attemptedCount = prepared.size() + (preparationFailure == null ? 0 : 1);
                transactions.executeWithoutResult(status -> release(claimed, attemptedCount));
            } catch (RuntimeException releaseFailure) {
                exception.addSuppressed(releaseFailure);
            }
            throw exception;
        }
        return new PreparedBatch(List.copyOf(prepared), preparationFailure);
    }

    private List<ClaimedEvent> claimBatch(int maxEvents, int maxBytes) {
        var events = transactions.execute(status -> {
            long now = System.currentTimeMillis();

            return jdbcTemplate.query(
                    CLAIM_SQL,
                    (resultSet, rowNum) -> new ClaimedEvent(
                            resultSet.getLong("id"),
                            resultSet.getString("run_id"),
                            resultSet.getString("source_id"),
                            UUID.fromString(resultSet.getString("event_id")),
                            ParsingMode.valueOf(resultSet.getString("mode")),
                            resultSet.getString("payload_json"),
                            resultSet.getString("result_json"),
                            resultSet.getInt("attempt_count")
                    ),
                    now, maxEvents, now, maxBytes
            );
        });

        // SQLite RETURNING의 행 순서는 보장되지 않는다.
        return Objects.requireNonNull(events).stream()
                .sorted(Comparator.comparingLong(ClaimedEvent::id))
                .toList();
    }

    private ObjectNode prepareResult(ClaimedEvent queued) {
        // 이미 결과가 보관되어 있다면 재파싱하지 않는다.
        if (queued.resultJson() != null) {
            var cached = mapper.readTree(queued.resultJson());

            if (cached instanceof ObjectNode object) {
                return object;
            }

            throw new IllegalStateException(
                    "Stored processing result must be a JSON object."
            );
        }

        var event = mapper.readValue(
                queued.payloadJson(),
                LogEventRequest.class
        );

        if (queued.mode() == ParsingMode.CENTRAL) {
            return event.logFormat() == null
                    ? processor.process(event.raw())
                    : processor.process(event.raw(), event.logFormat());
        }

        ObjectNode result = mapper.createObjectNode();
        result.put("raw", event.raw());
        result.put("parse_status", event.parseStatus().name());

        if (event.parseStatus() == ParseStatus.SUCCESS) {
            result.set("parsed", event.parsed().deepCopy());
        } else {
            ObjectNode error = mapper.createObjectNode();
            error.put("code", event.parseError().code());
            error.put("message", event.parseError().message());

            result.set("parse_error", error);
        }

        return result;
    }

    private void saveResults(List<PreparedLogEvent> events, List<Object[]> updates) {
        for (int updated : jdbcTemplate.batchUpdate(SAVE_SQL, updates)) {
            if (updated != 1) {
                throw new IllegalStateException("The claimed event could not be updated.");
            }
        }
        if (repairEnabled) {
            for (var event : events) {
                var result = event.result();
                if (ParseStatus.FAILED.name().equals(result.path("parse_status").stringValue())
                        && "INVALID_JSON".equals(result.at("/parse_error/code").stringValue())) {
                    repairJobs.register(event.runId(), event.sourceId(), event.eventId().toString());
                }
            }
        }
    }

    private void release(List<ClaimedEvent> events, int attemptedCount) {
        var updates = new ArrayList<Object[]>(events.size());
        for (int index = 0; index < events.size(); index++) {
            var event = events.get(index);
            updates.add(new Object[]{index < attemptedCount ? 0 : 1, event.id(), event.attemptCount()});
        }
        jdbcTemplate.batchUpdate(RELEASE_SQL, updates);
    }

    public record PreparedBatch(List<PreparedLogEvent> events, RuntimeException failure) {}

    private record ClaimedEvent(
            long id,
            String runId,
            String sourceId,
            UUID eventId,
            ParsingMode mode,
            String payloadJson,
            String resultJson,
            int attemptCount
    ) {}
}
