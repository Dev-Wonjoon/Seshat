package com.seshat.queue;

import com.seshat.ingestion.dto.LogBatchRequest;
import com.seshat.ingestion.dto.LogEventRequest;
import com.seshat.ingestion.dto.ParsingMode;
import com.seshat.ingestion.service.LogBatchStorageService;
import com.seshat.processing.JavaLogProcessor;
import com.seshat.storage.elasticsearch.BulkItemResult;
import com.seshat.storage.elasticsearch.ElasticsearchBulkWriter;
import com.seshat.storage.elasticsearch.ElasticsearchDocumentFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import tools.jackson.databind.json.JsonMapper;

import javax.sql.DataSource;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.*;

@SpringBootTest(properties = {
        "spring.datasource.url=jdbc:sqlite::memory:",
        "spring.datasource.hikari.maximum-pool-size=1",
        "spring.datasource.hikari.minimum-idle=1",
        "spring.datasource.hikari.pool-name=queue-worker-integration-test"
})
class QueueWorkerIntegrationTest {

    @Autowired
    private QueueWorker worker;

    @Autowired
    private QueueProcessingService processing;

    @Autowired
    private LogBatchStorageService storage;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Autowired
    private JsonMapper mapper;

    @Autowired
    private ElasticsearchDocumentFactory documents;

    @Autowired
    private DataSource dataSource;

    @MockitoBean
    private ElasticsearchBulkWriter writer;

    @MockitoSpyBean
    private JavaLogProcessor processor;

    private final String runId = "worker-integration-" + UUID.randomUUID();

    @AfterEach
    void cleanUp() {
        jdbcTemplate.update(
                "DELETE FROM log_queue WHERE run_id = ?",
                runId
        );
    }

    @Test
    void 일부_실패를_실제_DB의_개별_상태로_반영한다()
            throws IOException {
        var first = central("{\"message\":\"success\"}");
        var second = central("{\"message\":\"mapping failure\"}");
        var third = central("{\"message\":\"retry\"}");

        store(first, second, third);

        String originalPayload =
                (String) row(first).get("payload_json");

        when(writer.write(anyList())).thenAnswer(invocation -> {
            assertNoDatabaseTransaction();

            List<PreparedLogEvent> batch = invocation.getArgument(0);
            assertEquals(3, batch.size());

            // 전송 직전에는 모두 선점되고 파싱 결과도 보관되어 있다.
            for (var request : List.of(first, second, third)) {
                var queued = row(request);

                assertEquals("PROCESSING", queued.get("state"));
                assertNotNull(queued.get("result_json"));
            }

            return List.of(
                    result(batch.get(0), true, 201, null),
                    result(
                            batch.get(1),
                            false,
                            400,
                            "strict_dynamic_mapping_exception"
                    ),
                    result(
                            batch.get(2),
                            false,
                            429,
                            "es_rejected_execution_exception"
                    )
            );
        });

        assertEquals(3, worker.runOnce());

        assertState(first, "COMPLETED", 1);
        assertState(second, "STORAGE_FAILED", 1);
        assertState(third, "RETRY_WAIT", 1);

        var completed = row(first);
        var failed = row(second);
        var retrying = row(third);

        assertNull(completed.get("last_storage_error"));
        assertEquals(0L, number(completed, "next_attempt_at_ms"));

        assertTrue(
                ((String) failed.get("last_storage_error"))
                        .contains("strict_dynamic_mapping_exception")
        );
        assertEquals(0L, number(failed, "next_attempt_at_ms"));

        assertTrue(
                ((String) retrying.get("last_storage_error"))
                        .contains("es_rejected_execution_exception")
        );
        assertTrue(number(retrying, "next_attempt_at_ms") > 0);

        assertEquals(
                originalPayload,
                completed.get("payload_json")
        );
        assertEquals(
                first.raw(),
                mapper.readTree((String) completed.get("result_json"))
                        .path("raw")
                        .stringValue()
        );

        verify(writer, times(1)).write(anyList());
    }

    @Test
    void 연결_실패_후_기존_결과와_문서_ID로_재시도한다()
            throws IOException {
        var request = central("  {\"message\":\"retry me\"}  ");
        store(request);

        var sent = new ArrayList<PreparedLogEvent>();

        when(writer.write(anyList())).thenAnswer(invocation -> {
            assertNoDatabaseTransaction();

            List<PreparedLogEvent> batch = invocation.getArgument(0);
            assertEquals(1, batch.size());

            var event = batch.get(0);
            sent.add(event);

            if (sent.size() == 1) {
                throw new IOException("Connection refused.");
            }

            return List.of(result(event, true, 201, null));
        });

        var thrown = assertThrows(
                IllegalStateException.class,
                worker::runOnce
        );
        assertInstanceOf(IOException.class, thrown.getCause());

        assertState(request, "RETRY_WAIT", 1);

        var waiting = row(request);
        String originalPayload = (String) waiting.get("payload_json");
        String cachedResult = (String) waiting.get("result_json");

        assertTrue(
                ((String) waiting.get("last_storage_error"))
                        .contains("Connection refused.")
        );
        assertTrue(number(waiting, "next_attempt_at_ms") > 0);

        verify(processor).process(request.raw());
        clearInvocations(processor);

        // 실제로 기다리지 않고 재시도 가능한 시각으로 변경한다.
        jdbcTemplate.update(
                """
                UPDATE log_queue
                SET next_attempt_at_ms = 0
                WHERE run_id = ? AND event_id = ?
                """,
                runId,
                request.eventId().toString()
        );

        assertEquals(1, worker.runOnce());

        assertState(request, "COMPLETED", 2);

        var completed = row(request);

        assertEquals(originalPayload, completed.get("payload_json"));
        assertEquals(cachedResult, completed.get("result_json"));
        assertNull(completed.get("last_storage_error"));
        assertEquals(0L, number(completed, "next_attempt_at_ms"));

        assertEquals(2, sent.size());

        var firstAttempt = sent.get(0);
        var secondAttempt = sent.get(1);

        assertEquals(firstAttempt.queueId(), secondAttempt.queueId());
        assertEquals(firstAttempt.result(), secondAttempt.result());
        assertEquals(
                documents.documentId(firstAttempt),
                documents.documentId(secondAttempt)
        );
        assertEquals(1, firstAttempt.attemptCount());
        assertEquals(2, secondAttempt.attemptCount());

        verifyNoInteractions(processor);
        verify(writer, times(2)).write(anyList());
    }

    @Test
    void 만_건을_먼저_저장하고_초과분은_다음_배치에서_처리한다() throws IOException {
        var events = IntStream.rangeClosed(1, 10_001)
                .mapToObj(index -> central("{}"))
                .toList();
        for (int offset = 0; offset < events.size(); offset += 50) {
            var slice = events.subList(offset, Math.min(offset + 50, events.size()));
            storage.store(new LogBatchRequest(runId, "source-01", ParsingMode.CENTRAL, slice));
        }
        var sent = new ArrayList<UUID>();
        when(writer.write(anyList())).thenAnswer(invocation -> {
            List<PreparedLogEvent> batch = invocation.getArgument(0);
            batch.forEach(event -> sent.add(event.eventId()));
            return batch.stream().map(event -> result(event, true, 201, null)).toList();
        });

        assertEquals(10_000, worker.runOnce());
        assertState(events.get(9_999), "COMPLETED", 1);
        var remainder = row(events.getLast());
        assertEquals("PENDING", remainder.get("state"));
        assertEquals(0, number(remainder, "attempt_count"));

        assertEquals(1, worker.runOnce());
        assertEquals(events.stream().map(LogEventRequest::eventId).toList(), sent);
        assertEquals(10_001, jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM log_queue WHERE run_id = ? AND state = 'COMPLETED'",
                Integer.class, runId
        ));
    }

    @Test
    void 바이트_한도를_넘는_첫_이벤트도_단독으로_선점한다() {
        var first = central("{\"message\":\"" + "가".repeat(80) + "\"}");
        var second = central("{}");
        store(first, second);

        long beforeClaim = System.currentTimeMillis();
        var batch = processing.prepareBatch(10_000, 240);

        assertNull(batch.failure());
        assertEquals(List.of(first.eventId()), batch.events().stream().map(PreparedLogEvent::eventId).toList());
        assertTrue(number(row(first), "processing_started_at_ms") >= beforeClaim);
        assertEquals("PENDING", row(second).get("state"));
        assertEquals(0, number(row(second), "attempt_count"));
    }

    @Test
    void 준비_실패_후_파싱하지_않은_항목은_시도_횟수를_보존한다() {
        var first = central("{}");
        var second = central("{\"message\":\"fails\"}");
        var third = central("{\"message\":\"not reached\"}");
        store(first, second, third);
        doThrow(new IllegalStateException("Forced processing failure."))
                .when(processor).process(second.raw());

        assertThrows(IllegalStateException.class, worker::runOnce);

        assertState(first, "RETRY_WAIT", 1);
        assertEquals("PENDING", row(second).get("state"));
        assertEquals(1, number(row(second), "attempt_count"));
        assertEquals("PENDING", row(third).get("state"));
        assertEquals(0, number(row(third), "attempt_count"));
        verifyNoInteractions(writer);
    }

    private void assertNoDatabaseTransaction() {
        assertFalse(
                TransactionSynchronizationManager
                        .isActualTransactionActive()
        );
        assertFalse(
                TransactionSynchronizationManager.hasResource(dataSource)
        );

        // 연결 풀이 1개인 환경에서도 DB에 접근할 수 있어야 한다.
        assertEquals(
                1,
                jdbcTemplate.queryForObject("SELECT 1", Integer.class)
        );
    }

    private LogEventRequest central(String raw) {
        return new LogEventRequest(
                UUID.randomUUID(),
                raw,
                null,
                null,
                null
        );
    }

    private void store(LogEventRequest... events) {
        storage.store(new LogBatchRequest(
                runId,
                "source-01",
                ParsingMode.CENTRAL,
                List.of(events)
        ));
    }

    private BulkItemResult result(
            PreparedLogEvent event,
            boolean successful,
            int status,
            String errorType
    ) {
        return new BulkItemResult(
                event.queueId(),
                documents.documentId(event),
                successful,
                status,
                errorType,
                successful ? null : "Forced storage failure."
        );
    }

    private Map<String, Object> row(LogEventRequest event) {
        return jdbcTemplate.queryForMap(
                """
                SELECT *
                FROM log_queue
                WHERE run_id = ? AND event_id = ?
                """,
                runId,
                event.eventId().toString()
        );
    }

    private void assertState(
            LogEventRequest event,
            String expectedState,
            int expectedAttempts
    ) {
        var queued = row(event);

        assertEquals(expectedState, queued.get("state"));
        assertEquals(
                expectedAttempts,
                ((Number) queued.get("attempt_count")).intValue()
        );
        assertNull(queued.get("processing_started_at_ms"));
        assertNotNull(queued.get("result_json"));
    }

    private long number(Map<String, Object> row, String column) {
        return ((Number) row.get(column)).longValue();
    }
}
