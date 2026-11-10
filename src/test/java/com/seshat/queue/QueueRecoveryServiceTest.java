package com.seshat.queue;

import com.seshat.ingestion.dto.ParsingMode;
import com.seshat.processing.JavaLogProcessor;
import com.seshat.storage.elasticsearch.BulkItemResult;
import com.seshat.storage.elasticsearch.ElasticsearchDocumentFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;
import tools.jackson.databind.json.JsonMapper;

import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@SpringBootTest(properties = {
        "spring.datasource.url=jdbc:sqlite::memory:",
        "spring.datasource.hikari.maximum-pool-size=1",
        "spring.datasource.hikari.minimum-idle=1",
        "spring.datasource.hikari.pool-name=queue-recovery-test"
})
class QueueRecoveryServiceTest {

    @Autowired
    private QueueRecoveryService recovery;

    @Autowired
    private QueueProcessingService processing;

    @Autowired
    private QueueStorageResultService storageResults;

    @Autowired
    private ElasticsearchDocumentFactory documents;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Autowired
    private JsonMapper mapper;

    @MockitoSpyBean
    private JavaLogProcessor processor;

    private final String runId = "recovery-test-" + UUID.randomUUID();

    @AfterEach
    void cleanUp() {
        jdbcTemplate.update(
                "DELETE FROM log_queue WHERE run_id = ?",
                runId
        );
    }

    @Test
    void 처리_중인_이벤트만_회수하고_보관_내용은_유지한다() {
        var interrupted = insert("PROCESSING", true);
        var before = row(interrupted);

        var untouched = List.of(
                insert("PENDING", true),
                insert("RETRY_WAIT", true),
                insert("COMPLETED", true),
                insert("STORAGE_FAILED", true)
        );
        var snapshots = untouched.stream()
                .map(this::row)
                .toList();

        assertEquals(1, recovery.recoverInterrupted());

        var after = row(interrupted);

        assertEquals("PENDING", after.get("state"));
        assertNull(after.get("processing_started_at_ms"));
        assertEquals(
                0L,
                ((Number) after.get("next_attempt_at_ms")).longValue()
        );

        for (String column : List.of(
                "id", "run_id", "source_id", "event_id", "mode",
                "payload_json", "result_json", "received_at_ms",
                "attempt_count", "last_storage_error"
        )) {
            assertEquals(before.get(column), after.get(column), column);
        }

        for (int index = 0; index < untouched.size(); index++) {
            assertEquals(
                    snapshots.get(index),
                    row(untouched.get(index))
            );
        }

        // 이미 회수한 상태에서 다시 호출해도 추가 변경이 없다.
        assertEquals(0, recovery.recoverInterrupted());
        assertEquals(after, row(interrupted));

        verifyNoInteractions(processor);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void 회수_후_새_시도로_처리하고_이전_응답은_무시한다(
            boolean cached
    ) {
        var oldAttempt = insert("PROCESSING", cached);
        var before = row(oldAttempt);
        var oldResponse = success(oldAttempt);

        assertEquals(1, recovery.recoverInterrupted());

        // 회수 직후에는 PENDING이므로 이전 응답을 반영하지 않는다.
        assertFalse(storageResults.apply(oldAttempt, oldResponse));
        assertEquals("PENDING", row(oldAttempt).get("state"));

        var current = processing.prepareNext().orElseThrow();

        assertEquals(oldAttempt.queueId(), current.queueId());
        assertEquals(oldAttempt.eventId(), current.eventId());
        assertEquals(4, current.attemptCount());
        assertEquals(oldAttempt.result(), current.result());

        assertEquals(
                documents.documentId(oldAttempt),
                documents.documentId(current)
        );

        var prepared = row(current);

        assertEquals("PROCESSING", prepared.get("state"));
        assertEquals(before.get("payload_json"), prepared.get("payload_json"));
        assertEquals(
                current.result(),
                mapper.readTree((String) prepared.get("result_json"))
        );

        if (cached) {
            assertEquals(
                    before.get("result_json"),
                    prepared.get("result_json")
            );
            verifyNoInteractions(processor);
        } else {
            verify(processor).process(
                    current.result().path("raw").stringValue()
            );
        }

        // 재선점 후에도 이전 시도 번호의 응답은 반영하지 않는다.
        assertFalse(storageResults.apply(oldAttempt, oldResponse));
        assertEquals(prepared, row(current));

        assertTrue(storageResults.apply(current, success(current)));

        var completed = row(current);
        assertEquals("COMPLETED", completed.get("state"));
        assertNull(completed.get("processing_started_at_ms"));
        assertNull(completed.get("last_storage_error"));
    }

    private PreparedLogEvent insert(String state, boolean cached) {
        UUID eventId = UUID.randomUUID();
        String raw = "  {\"message\":\"recover\"}  ";

        var payload = mapper.createObjectNode();
        payload.put("event_id", eventId.toString());
        payload.put("raw", raw);

        var result = mapper.createObjectNode();
        result.put("raw", raw);
        result.put("parse_status", "SUCCESS");
        result.set("parsed", mapper.readTree(raw));

        jdbcTemplate.update(
                """
                INSERT INTO log_queue (
                    run_id, source_id, event_id, mode,
                    payload_json, result_json, state,
                    received_at_ms, processing_started_at_ms,
                    attempt_count, next_attempt_at_ms,
                    last_storage_error
                )
                VALUES (
                    ?, ?, ?, 'CENTRAL',
                    ?, ?, ?,
                    ?, ?, 3, ?, ?
                )
                """,
                runId,
                "source-01",
                eventId.toString(),
                mapper.writeValueAsString(payload),
                cached ? mapper.writeValueAsString(result) : null,
                state,
                100L,
                state.equals("PROCESSING") ? 200L : null,
                state.equals("RETRY_WAIT") ? Long.MAX_VALUE : 0L,
                state.equals("COMPLETED")
                        ? null
                        : "Previous storage failure."
        );

        Long queueId = jdbcTemplate.queryForObject(
                """
                SELECT id
                FROM log_queue
                WHERE run_id = ? AND event_id = ?
                """,
                Long.class,
                runId,
                eventId.toString()
        );

        return new PreparedLogEvent(
                queueId,
                runId,
                "source-01",
                eventId,
                ParsingMode.CENTRAL,
                result,
                3
        );
    }

    private BulkItemResult success(PreparedLogEvent event) {
        return new BulkItemResult(
                event.queueId(),
                documents.documentId(event),
                true,
                201,
                null,
                null
        );
    }

    private Map<String, Object> row(PreparedLogEvent event) {
        return jdbcTemplate.queryForMap(
                """
                SELECT *
                FROM log_queue
                WHERE id = ? AND run_id = ?
                """,
                event.queueId(),
                runId
        );
    }
}