package com.seshat.queue;

import com.seshat.ingestion.dto.ParsingMode;
import com.seshat.storage.elasticsearch.BulkItemResult;
import com.seshat.storage.elasticsearch.ElasticsearchDocumentFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import tools.jackson.databind.json.JsonMapper;

import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest(properties = {
        "spring.datasource.url=jdbc:sqlite::memory:",
        "spring.datasource.hikari.maximum-pool-size=1",
        "spring.datasource.hikari.minimum-idle=1",
        "spring.datasource.hikari.pool-name=queue-storage-result-test"
})
class QueueStorageResultServiceTest {

    @Autowired
    private QueueStorageResultService service;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Autowired
    private ElasticsearchDocumentFactory documents;

    @Autowired
    private JsonMapper mapper;

    private final String runId = "storage-result-test-" + UUID.randomUUID();

    @AfterEach
    void cleanUp() {
        jdbcTemplate.update(
                "DELETE FROM log_queue WHERE run_id = ?",
                runId
        );
    }

    @Test
    void 저장_성공은_완료로_변경하고_이전_오류를_지운다() {
        var event = prepared(1);
        var before = row(event);
        var result = result(event, true, 201, null, null);

        assertTrue(service.apply(event, result));

        var after = row(event);
        assertTransition(before, after, "COMPLETED");
        assertEquals(0L, number(after, "next_attempt_at_ms"));
        assertNull(after.get("last_storage_error"));

        // 같은 성공 응답을 다시 반영해도 완료 상태를 건드리지 않는다.
        assertFalse(service.apply(event, result));
        assertEquals(after, row(event));
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "document_parsing_exception",
            "mapper_parsing_exception",
            "strict_dynamic_mapping_exception"
    })
    void 확인된_매핑_오류는_영구_실패로_보관한다(String errorType) {
        var event = prepared(1);
        var before = row(event);

        assertTrue(service.apply(
                event,
                result(event, false, 400, errorType, "Mapping rejected.")
        ));

        var after = row(event);
        assertTransition(before, after, "STORAGE_FAILED");
        assertEquals(0L, number(after, "next_attempt_at_ms"));
        assertEquals(
                "400 / " + errorType + ": Mapping rejected.",
                after.get("last_storage_error")
        );
    }

    @ParameterizedTest
    @CsvSource({
            "429, es_rejected_execution_exception",
            "503, unavailable_shards_exception",
            "403, security_exception",
            "400, unknown_error"
    })
    void 그_외_오류는_재시도_대기로_보관한다(
            int status,
            String errorType
    ) {
        var event = prepared(1);
        var before = row(event);

        long startedAt = System.currentTimeMillis();

        assertTrue(service.apply(
                event,
                result(event, false, status, errorType, "Storage rejected.")
        ));

        long finishedAt = System.currentTimeMillis();

        var after = row(event);
        assertTransition(before, after, "RETRY_WAIT");
        assertRetryTime(after, startedAt, finishedAt, 1_000L);
        assertEquals(
                status + " / " + errorType + ": Storage rejected.",
                after.get("last_storage_error")
        );
    }

    @ParameterizedTest
    @CsvSource({
            "1, 1000",
            "2, 2000",
            "3, 4000",
            "6, 32000",
            "7, 60000",
            "100, 60000"
    })
    void 연결_실패는_간격을_늘려_재시도하고_60초로_제한한다(
            int attempts,
            long expectedDelay
    ) {
        var event = prepared(attempts);
        var before = row(event);

        long startedAt = System.currentTimeMillis();

        assertTrue(service.retryAfterFailure(
                event, "Connection refused."
        ));

        long finishedAt = System.currentTimeMillis();

        var after = row(event);
        assertTransition(before, after, "RETRY_WAIT");
        assertRetryTime(after, startedAt, finishedAt, expectedDelay);
        assertEquals(
                "Connection refused.",
                after.get("last_storage_error")
        );
    }

    @Test
    void 이전_시도의_성공과_실패는_현재_상태를_변경하지_않는다() {
        var oldEvent = prepared(1);

        jdbcTemplate.update(
                """
                UPDATE log_queue
                SET attempt_count = 2
                WHERE id = ? AND run_id = ?
                """,
                oldEvent.queueId(),
                runId
        );

        var before = row(oldEvent);

        assertFalse(service.apply(
                oldEvent,
                result(oldEvent, true, 200, null, null)
        ));

        assertFalse(service.retryAfterFailure(
                oldEvent, "Late connection failure."
        ));

        assertEquals(before, row(oldEvent));
    }

    @Test
    void 다른_문서의_응답은_거절하고_DB를_유지한다() {
        var event = prepared(1);
        var before = row(event);

        var wrongQueue = new BulkItemResult(
                event.queueId() + 1,
                documents.documentId(event),
                true, 201, null, null
        );

        var wrongDocument = new BulkItemResult(
                event.queueId(),
                "different-document-id",
                true, 201, null, null
        );

        assertThrows(
                IllegalArgumentException.class,
                () -> service.apply(event, wrongQueue)
        );
        assertThrows(
                IllegalArgumentException.class,
                () -> service.apply(event, wrongDocument)
        );

        assertEquals(before, row(event));
    }

    private PreparedLogEvent prepared(int attempts) {
        UUID eventId = UUID.randomUUID();
        String sourceId = "source-01";
        String raw = "  {\"message\":\"안녕\"}  ";

        var payload = mapper.createObjectNode();
        payload.put("event_id", eventId.toString());
        payload.put("raw", raw);

        var result = mapper.createObjectNode();
        result.put("raw", raw);
        result.put("parse_status", "SUCCESS");
        result.set("parsed", mapper.readTree(raw));

        long now = System.currentTimeMillis();

        jdbcTemplate.update(
                """
                INSERT INTO log_queue (
                    run_id, source_id, event_id, mode,
                    payload_json, result_json, state,
                    received_at_ms, processing_started_at_ms,
                    attempt_count, next_attempt_at_ms, last_storage_error
                )
                VALUES (
                    ?, ?, ?, 'CENTRAL',
                    ?, ?, 'PROCESSING',
                    ?, ?, ?, ?, ?
                )
                """,
                runId,
                sourceId,
                eventId.toString(),
                mapper.writeValueAsString(payload),
                mapper.writeValueAsString(result),
                now,
                now,
                attempts,
                123L,
                "Previous storage failure."
        );

        Long queueId = jdbcTemplate.queryForObject(
                """
                SELECT id FROM log_queue
                WHERE run_id = ? AND source_id = ? AND event_id = ?
                """,
                Long.class,
                runId,
                sourceId,
                eventId.toString()
        );

        return new PreparedLogEvent(
                queueId, runId, sourceId, eventId,
                ParsingMode.CENTRAL, result, attempts
        );
    }

    private BulkItemResult result(
            PreparedLogEvent event,
            boolean successful,
            int status,
            String errorType,
            String errorReason
    ) {
        return new BulkItemResult(
                event.queueId(),
                documents.documentId(event),
                successful,
                status,
                errorType,
                errorReason
        );
    }

    private Map<String, Object> row(PreparedLogEvent event) {
        return jdbcTemplate.queryForMap(
                "SELECT * FROM log_queue WHERE id = ? AND run_id = ?",
                event.queueId(),
                runId
        );
    }

    private void assertTransition(
            Map<String, Object> before,
            Map<String, Object> after,
            String expectedState
    ) {
        assertEquals(expectedState, after.get("state"));
        assertNull(after.get("processing_started_at_ms"));

        // 상태 변경 중에도 식별 정보와 원문·결과를 보존한다.
        for (String column : new String[]{
                "id", "run_id", "source_id", "event_id", "mode",
                "payload_json", "result_json",
                "received_at_ms", "attempt_count"
        }) {
            assertEquals(before.get(column), after.get(column), column);
        }
    }

    private void assertRetryTime(
            Map<String, Object> stored,
            long startedAt,
            long finishedAt,
            long expectedDelay
    ) {
        long nextAttemptAt = number(stored, "next_attempt_at_ms");

        assertTrue(nextAttemptAt >= startedAt + expectedDelay);
        assertTrue(nextAttemptAt <= finishedAt + expectedDelay);
    }

    private long number(Map<String, Object> stored, String column) {
        return ((Number) stored.get(column)).longValue();
    }
}