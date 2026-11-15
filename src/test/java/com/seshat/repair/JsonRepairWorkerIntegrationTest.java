package com.seshat.repair;

import com.seshat.ingestion.dto.LogBatchRequest;
import com.seshat.ingestion.dto.LogEventRequest;
import com.seshat.ingestion.dto.ParseError;
import com.seshat.ingestion.dto.ParseStatus;
import com.seshat.ingestion.dto.ParsingMode;
import com.seshat.ingestion.service.LogBatchStorageService;
import com.seshat.queue.PreparedLogEvent;
import com.seshat.queue.QueueRecoveryService;
import com.seshat.queue.QueueWorker;
import com.seshat.storage.elasticsearch.BulkItemResult;
import com.seshat.storage.elasticsearch.ElasticsearchBulkWriter;
import com.seshat.storage.elasticsearch.ElasticsearchDocumentFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static com.seshat.repair.RepairCandidate.Decision.CANDIDATE;
import static com.seshat.repair.RepairCandidate.Decision.UNRECOVERABLE;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@SpringBootTest(properties = {
        "spring.datasource.url=jdbc:sqlite::memory:",
        "spring.datasource.hikari.maximum-pool-size=1",
        "spring.datasource.hikari.minimum-idle=1",
        "spring.datasource.hikari.connection-timeout=2000",
        "seshat.repair.enabled=true",
        "seshat.repair.scheduler-enabled=false",
        "seshat.repair.provider=localRepair",
        "seshat.repair.max-attempts=2",
        "seshat.repair.retry-delay-ms=60000"
})
class JsonRepairWorkerIntegrationTest {

    private static final String RAW = "{\"a\":1 \"b\":2}";
    private static final String REPAIRED = "{\"a\":1,\"b\":2}";

    @Autowired private JdbcTemplate jdbc;
    @Autowired private JsonRepairWorker repair;
    @Autowired private JsonRepairRepository repository;
    @Autowired private QueueWorker queue;
    @Autowired private QueueRecoveryService recovery;
    @Autowired private LogBatchStorageService storage;
    @Autowired private ElasticsearchDocumentFactory documents;

    @MockitoBean(name = "localRepair") private JsonRepairClient local;
    @MockitoBean(name = "externalRepair") private JsonRepairClient external;
    @MockitoBean private ElasticsearchBulkWriter writer;

    @BeforeEach
    void setUp() throws IOException {
        when(local.profile()).thenReturn(new JsonRepairClient.Profile("local-host", "local-model", "v1"));
        when(external.profile()).thenReturn(new JsonRepairClient.Profile("external-host", "external-model", "v1"));
        when(writer.write(anyList())).thenAnswer(invocation -> {
            List<PreparedLogEvent> batch = invocation.getArgument(0);
            return batch.stream().map(event -> new BulkItemResult(
                    event.queueId(), documents.documentId(event), true, 201, null, null
            )).toList();
        });
    }

    @AfterEach
    void cleanUp() {
        jdbc.update("DELETE FROM json_repair_attempt");
        jdbc.update("DELETE FROM json_repair_job");
        jdbc.update("DELETE FROM log_queue");
    }

    @Test
    void 두_모드에서_등록하고_중앙과_에이전트의_최초_실패를_보존한다() throws Exception {
        UUID central = store(ParsingMode.CENTRAL, RAW);
        UUID agent = store(ParsingMode.AGENT, RAW);
        assertEquals(2, queue.runOnce());

        String centralResult = result(central);
        String agentResult = result(agent);
        when(local.repair(any())).thenReturn(new RepairCandidate(CANDIDATE, REPAIRED, null, "응답"));

        assertTrue(repair.runOnce());
        assertTrue(repair.runOnce());
        assertFalse(repair.runOnce());
        assertEquals("VERIFIED", job(central).get("state"));
        assertEquals("VERIFIED", job(agent).get("state"));
        assertEquals(REPAIRED, attempt(central, 1).get("candidate_json"));
        assertEquals("local-model", attempt(agent, 1).get("model"));
        assertEquals("localRepair", attempt(agent, 1).get("provider"));
        assertEquals("local-host", attempt(agent, 1).get("target_id"));
        assertEquals("v1", attempt(agent, 1).get("prompt_version"));
        assertEquals(centralResult, result(central));
        assertEquals(agentResult, result(agent));
        assertTrue(agentResult.contains("에이전트 최초 오류"));
        verify(local).repair(argThat(request -> request.raw().equals(RAW)
                && request.errorCode().equals("INVALID_JSON")
                && request.errorMessage().equals("에이전트 최초 오류")));
        verifyNoInteractions(external);

        jdbc.update("UPDATE log_queue SET state = 'PROCESSING'");
        assertEquals(2, recovery.recoverInterrupted());
        assertEquals(2, queue.runOnce());
        assertEquals(2, jdbc.queryForObject("SELECT count(*) FROM json_repair_job", Integer.class));
        assertEquals("VERIFIED", job(central).get("state"));
    }

    @Test
    void 모의_모델의_복구_불가와_후보_검증_실패를_구분한다() throws Exception {
        UUID cannotRepair = store(ParsingMode.CENTRAL, RAW);
        UUID invalidSyntax = store(ParsingMode.CENTRAL, RAW);
        UUID changedValue = store(ParsingMode.CENTRAL, RAW);
        queue.runOnce();

        when(local.repair(any())).thenReturn(
                new RepairCandidate(UNRECOVERABLE, null, "모호함", "첫 응답"),
                new RepairCandidate(CANDIDATE, "{\"a\":", null, "둘째 응답"),
                new RepairCandidate(CANDIDATE, "{\"a\":1,\"b\":9}", "후보 이유", "셋째 응답")
        );
        assertTrue(repair.runOnce());
        assertTrue(repair.runOnce());
        assertTrue(repair.runOnce());
        assertFalse(repair.runOnce());

        assertEquals("UNRECOVERABLE", job(cannotRepair).get("state"));
        assertEquals("모호함", job(cannotRepair).get("reason"));
        assertEquals("CALL_FAILED", job(invalidSyntax).get("state"));
        assertEquals("INVALID_CANDIDATE_SYNTAX", job(invalidSyntax).get("reason"));
        assertEquals(0, attempt(invalidSyntax, 1).get("syntax_valid"));
        assertNull(attempt(invalidSyntax, 1).get("values_preserved"));
        assertEquals("CANDIDATE_REJECTED", job(changedValue).get("state"));
        assertEquals(1, attempt(changedValue, 1).get("syntax_valid"));
        assertEquals(0, attempt(changedValue, 1).get("values_preserved"));
        assertEquals("후보 이유", attempt(changedValue, 1).get("model_reason"));
        assertEquals("CANDIDATE", attempt(changedValue, 1).get("model_decision"));
    }

    @Test
    void 시간_초과는_한도까지_재시도하고_중단된_시도는_회수한다() throws Exception {
        UUID event = store(ParsingMode.CENTRAL, RAW);
        queue.runOnce();
        when(local.repair(any())).thenThrow(new RepairCallException(RepairCallException.Reason.TIMEOUT));

        assertTrue(repair.runOnce());
        assertEquals("RETRY_WAIT", job(event).get("state"));
        assertEquals("TIMEOUT", attempt(event, 1).get("reason"));
        assertFalse(repair.runOnce());
        jdbc.update("UPDATE json_repair_job SET next_attempt_at_ms = 0");
        assertTrue(repair.runOnce());
        assertEquals("CALL_FAILED", job(event).get("state"));
        assertEquals("TIMEOUT", attempt(event, 2).get("reason"));

        UUID interrupted = store(ParsingMode.CENTRAL, RAW);
        queue.runOnce();
        var claimed = repository.claim(1, 2, 2, "localRepair", local.profile()).orElseThrow();
        assertTrue(repair.runOnce());
        assertEquals("INTERRUPTED", attempt(interrupted, 1).get("reason"));
        assertEquals("CALL_FAILED", job(interrupted).get("state"));
        repository.finish(claimed, JsonRepairRepository.Outcome.withoutCandidate("VERIFIED", null),
                System.currentTimeMillis(), 0);
        assertEquals("INTERRUPTED", attempt(interrupted, 1).get("reason"));
        assertFalse(repair.runOnce());
    }

    @Test
    void 복구_호출이_대기_중이어도_정상_수집과_ES_저장이_완료된다() throws Exception {
        UUID broken = store(ParsingMode.CENTRAL, RAW);
        queue.runOnce();
        var entered = new CountDownLatch(1);
        var resume = new CountDownLatch(1);
        when(local.repair(any())).thenAnswer(invocation -> {
            assertFalse(TransactionSynchronizationManager.isActualTransactionActive());
            entered.countDown();
            assertTrue(resume.await(5, TimeUnit.SECONDS));
            return new RepairCandidate(CANDIDATE, REPAIRED, null, null);
        });

        try (var executor = Executors.newSingleThreadExecutor()) {
            var running = executor.submit(repair::runOnce);
            try {
                assertTrue(entered.await(3, TimeUnit.SECONDS));
                UUID valid = store(ParsingMode.CENTRAL, "{\"healthy\":true}");
                assertEquals(1, queue.runOnce());
                assertEquals("COMPLETED", jdbc.queryForObject(
                        "SELECT state FROM log_queue WHERE event_id = ?", String.class, valid.toString()));
                assertEquals("PROCESSING", job(broken).get("state"));
                assertEquals(1, jdbc.queryForObject("SELECT count(*) FROM json_repair_job", Integer.class));
            } finally {
                resume.countDown();
            }
            assertTrue(running.get(3, TimeUnit.SECONDS));
        }
        assertEquals("VERIFIED", job(broken).get("state"));
    }

    @Test
    void 제외_입력은_모델을_호출하지_않고_재시도의_공급자도_바꾸지_않는다() throws Exception {
        UUID plain = store(ParsingMode.CENTRAL, "daemon[321]: ready");
        UUID duplicate = store(ParsingMode.CENTRAL, "{\"a\":1,\"a\":1}");
        UUID unsupported = store(ParsingMode.CENTRAL, "[1]");
        UUID oldProfile = store(ParsingMode.CENTRAL, RAW);
        queue.runOnce();
        assertTrue(repair.runOnce());
        assertTrue(repair.runOnce());
        repository.claim(0, 1, 2, "externalRepair",
                new JsonRepairClient.Profile("old-host", "external-model", "v1")).orElseThrow();
        assertTrue(repair.runOnce());

        assertEquals("SKIPPED", job(plain).get("state"));
        assertEquals("DUPLICATE_KEY", job(duplicate).get("reason"));
        assertEquals("PROVIDER_PROFILE_UNAVAILABLE", job(oldProfile).get("reason"));
        assertEquals(0, jdbc.queryForObject(
                "SELECT count(*) FROM json_repair_job WHERE event_id = ?", Integer.class, unsupported.toString()));
        verify(local, never()).repair(any());
        verify(external, never()).repair(any());
    }

    private UUID store(ParsingMode mode, String raw) {
        UUID id = UUID.randomUUID();
        var event = mode == ParsingMode.CENTRAL
                ? new LogEventRequest(id, raw, null, null, null)
                : new LogEventRequest(id, raw, ParseStatus.FAILED, null,
                        new ParseError("INVALID_JSON", "에이전트 최초 오류"));
        storage.store(new LogBatchRequest("repair-run", "test-source", mode, List.of(event)));
        return id;
    }

    private String result(UUID event) {
        return jdbc.queryForObject("SELECT result_json FROM log_queue WHERE event_id = ?",
                String.class, event.toString());
    }

    private Map<String, Object> job(UUID event) {
        return jdbc.queryForMap("SELECT * FROM json_repair_job WHERE event_id = ?", event.toString());
    }

    private Map<String, Object> attempt(UUID event, int number) {
        return jdbc.queryForMap("""
                SELECT attempt.* FROM json_repair_attempt attempt
                JOIN json_repair_job job ON job.id = attempt.job_id
                WHERE job.event_id = ? AND attempt.attempt_number = ?
                """, event.toString(), number);
    }
}
