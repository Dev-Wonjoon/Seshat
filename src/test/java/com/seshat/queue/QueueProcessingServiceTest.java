package com.seshat.queue;

import com.seshat.ingestion.dto.LogBatchRequest;
import com.seshat.ingestion.dto.LogEventRequest;
import com.seshat.ingestion.dto.ParseError;
import com.seshat.ingestion.dto.ParseStatus;
import com.seshat.ingestion.dto.ParsingMode;
import com.seshat.ingestion.service.LogBatchStorageService;
import com.seshat.processing.JavaLogProcessor;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
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
        "spring.datasource.hikari.pool-name=queue-processing-test"
})
class QueueProcessingServiceTest {

    @Autowired
    private QueueProcessingService worker;

    @Autowired
    private LogBatchStorageService storage;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Autowired
    private JsonMapper mapper;

    @Autowired
    private QueueStorageResultService storageResults;

    @MockitoSpyBean
    private JavaLogProcessor processor;

    private final String runId = "queue-test-" + UUID.randomUUID();

    @AfterEach
    void cleanUp() {
        // 이 테스트가 만든 메모리 DB 레코드만 정리한다.
        jdbcTemplate.update(
                "DELETE FROM log_queue WHERE run_id = ?",
                runId
        );
    }

    @Test
    void 재시도_시간이_남은_이벤트는_가져오지_않는다() {
        String raw = "{\"message\":\"waiting\"}";
        store(ParsingMode.CENTRAL, central(raw));

        var first = worker.prepareNext().orElseThrow();

        assertTrue(storageResults.retryAfterFailure(
                first, "Connection refused."
        ));

        jdbcTemplate.update(
                """
                UPDATE log_queue
                SET next_attempt_at_ms = ?
                WHERE id = ? AND run_id = ?
                """,
                Long.MAX_VALUE,
                first.queueId(),
                runId
        );

        var before = row();
        clearInvocations(processor);

        assertTrue(worker.prepareNext().isEmpty());

        assertEquals(before, row());
        verifyNoInteractions(processor);
    }

    @Test
    void 재시도_시간이_지나면_기존_결과로_다시_처리한다() {
        String raw = "  {\"message\":\"retry\"}  ";
        store(ParsingMode.CENTRAL, central(raw));

        var first = worker.prepareNext().orElseThrow();
        String originalPayload = (String) row().get("payload_json");
        String originalResult = (String) row().get("result_json");

        assertTrue(storageResults.retryAfterFailure(
                first, "Connection refused."
        ));

        jdbcTemplate.update(
                """
                UPDATE log_queue
                SET next_attempt_at_ms = 0
                WHERE id = ? AND run_id = ?
                """,
                first.queueId(),
                runId
        );

        clearInvocations(processor);

        var retried = worker.prepareNext().orElseThrow();

        assertEquals(first.queueId(), retried.queueId());
        assertEquals(first.eventId(), retried.eventId());
        assertEquals(first.result(), retried.result());
        assertEquals(2, retried.attemptCount());

        assertPrepared(retried, 2);

        var stored = row();
        assertEquals(originalPayload, stored.get("payload_json"));
        assertEquals(originalResult, stored.get("result_json"));
        assertEquals(
                0L,
                ((Number) stored.get("next_attempt_at_ms")).longValue()
        );

        verifyNoInteractions(processor);

        // 재처리 중인 항목을 중복으로 가져오지 않는다.
        assertTrue(worker.prepareNext().isEmpty());
    }

    @Test
    void 재시도를_기다리는_이벤트가_새_이벤트를_막지_않는다() {
        store(
                ParsingMode.CENTRAL,
                central("{\"message\":\"waiting\"}")
        );

        var waiting = worker.prepareNext().orElseThrow();

        assertTrue(storageResults.retryAfterFailure(
                waiting, "Connection refused."
        ));

        jdbcTemplate.update(
                """
                UPDATE log_queue
                SET next_attempt_at_ms = ?
                WHERE id = ? AND run_id = ?
                """,
                Long.MAX_VALUE,
                waiting.queueId(),
                runId
        );

        var nextEvent = new LogEventRequest(
                new UUID(0, 2),
                "{\"message\":\"new\"}",
                null,
                null,
                null
        );

        store(ParsingMode.CENTRAL, nextEvent);

        var prepared = worker.prepareNext().orElseThrow();

        assertEquals(nextEvent.eventId(), prepared.eventId());
        assertEquals(1, prepared.attemptCount());
        assertEquals(
                "new",
                prepared.result().at("/parsed/message").stringValue()
        );

        String waitingState = jdbcTemplate.queryForObject(
                """
                SELECT state FROM log_queue
                WHERE id = ? AND run_id = ?
                """,
                String.class,
                waiting.queueId(),
                runId
        );

        assertEquals("RETRY_WAIT", waitingState);
        assertTrue(worker.prepareNext().isEmpty());
    }

    @Test
    void 빈_큐에서는_처리할_이벤트가_없다() {
        assertTrue(worker.prepareNext().isEmpty());
        verifyNoInteractions(processor);
    }

    @Test
    void 중앙_모드는_파싱하고_수신_원문을_유지한다() {
        String raw = "  {\"level\":\"info\",\"message\":\"안녕\"}  ";
        store(ParsingMode.CENTRAL, central(raw));

        String originalPayload = (String) row().get("payload_json");

        var prepared = worker.prepareNext().orElseThrow();

        assertEquals(ParsingMode.CENTRAL, prepared.mode());
        assertEquals(
                "SUCCESS",
                prepared.result().path("parse_status").stringValue()
        );
        assertEquals(raw, prepared.result().path("raw").stringValue());
        assertEquals(
                "info",
                prepared.result().at("/parsed/level").stringValue()
        );

        assertEquals(originalPayload, row().get("payload_json"));
        assertPrepared(prepared, 1);

        verify(processor).process(raw);

        // 이미 PROCESSING인 이벤트를 다시 가져오지 않는다.
        assertTrue(worker.prepareNext().isEmpty());
    }

    @Test
    void 중앙_파싱_실패도_오류_결과를_보관한다() {
        String raw = "{\"message\":";
        store(ParsingMode.CENTRAL, central(raw));

        var prepared = worker.prepareNext().orElseThrow();

        assertEquals(
                "FAILED",
                prepared.result().path("parse_status").stringValue()
        );
        assertEquals(
                "INVALID_JSON",
                prepared.result().at("/parse_error/code").stringValue()
        );
        assertEquals(raw, prepared.result().path("raw").stringValue());
        assertFalse(prepared.result().has("parsed"));

        // 파싱 실패는 ES 저장 실패 상태로 바꾸지 않는다.
        assertPrepared(prepared, 1);
        verify(processor).process(raw);
    }

    @Test
    void 에이전트_성공_결과는_재파싱하지_않는다() {
        String raw = "{\"level\":\"info\",\"message\":\"ok\"}";
        var parsed = mapper.readTree(raw);

        var event = new LogEventRequest(
                eventId(),
                raw,
                ParseStatus.SUCCESS,
                parsed,
                null
        );

        store(ParsingMode.AGENT, event);

        var prepared = worker.prepareNext().orElseThrow();

        assertEquals(ParsingMode.AGENT, prepared.mode());
        assertEquals(
                "SUCCESS",
                prepared.result().path("parse_status").stringValue()
        );
        assertEquals(parsed, prepared.result().path("parsed"));
        assertEquals(raw, prepared.result().path("raw").stringValue());
        assertFalse(prepared.result().has("parse_error"));

        assertPrepared(prepared, 1);
        verifyNoInteractions(processor);
    }

    @Test
    void 에이전트_실패_결과도_재파싱하지_않는다() {
        String raw = "{\"message\":";

        var event = new LogEventRequest(
                eventId(),
                raw,
                ParseStatus.FAILED,
                null,
                new ParseError("INVALID_JSON", "Agent parsing failed.")
        );

        store(ParsingMode.AGENT, event);

        var prepared = worker.prepareNext().orElseThrow();

        assertEquals(
                "FAILED",
                prepared.result().path("parse_status").stringValue()
        );
        assertEquals(
                "INVALID_JSON",
                prepared.result().at("/parse_error/code").stringValue()
        );
        assertEquals(
                "Agent parsing failed.",
                prepared.result().at("/parse_error/message").stringValue()
        );
        assertEquals(raw, prepared.result().path("raw").stringValue());
        assertFalse(prepared.result().has("parsed"));

        assertPrepared(prepared, 1);
        verifyNoInteractions(processor);
    }

    @Test
    void 이미_보관된_결과는_재사용한다() {
        String raw = "{\"message\":\"cached\"}";
        store(ParsingMode.CENTRAL, central(raw));

        var cached = mapper.createObjectNode();
        cached.put("raw", raw);
        cached.put("parse_status", "SUCCESS");
        cached.set("parsed", mapper.readTree(raw));

        jdbcTemplate.update(
                """
                UPDATE log_queue
                SET result_json = ?
                WHERE run_id = ?
                """,
                mapper.writeValueAsString(cached),
                runId
        );

        var prepared = worker.prepareNext().orElseThrow();

        assertEquals(cached, prepared.result());
        assertPrepared(prepared, 1);
        verifyNoInteractions(processor);
    }

    @Test
    void 예기치_않은_오류는_대기로_돌리고_다시_시도할_수_있다() {
        String raw = "{\"message\":\"retry\"}";
        store(ParsingMode.CENTRAL, central(raw));

        doThrow(new IllegalStateException("Forced processing failure."))
                .when(processor).process(raw);

        assertThrows(
                IllegalStateException.class,
                () -> worker.prepareNext()
        );

        var failedAttempt = row();

        assertEquals("PENDING", failedAttempt.get("state"));
        assertNull(failedAttempt.get("processing_started_at_ms"));
        assertNull(failedAttempt.get("result_json"));
        assertEquals(
                1,
                ((Number) failedAttempt.get("attempt_count")).intValue()
        );

        // 다음 시도에서는 실제 파서가 실행되도록 복원한다.
        doCallRealMethod().when(processor).process(raw);

        var prepared = worker.prepareNext().orElseThrow();

        assertEquals(
                "SUCCESS",
                prepared.result().path("parse_status").stringValue()
        );
        assertPrepared(prepared, 2);
    }

    private UUID eventId() {
        return new UUID(0, 1);
    }

    private LogEventRequest central(String raw) {
        return new LogEventRequest(
                eventId(), raw, null, null, null
        );
    }

    private void store(
            ParsingMode mode,
            LogEventRequest event
    ) {
        storage.store(new LogBatchRequest(
                runId,
                "source-01",
                mode,
                List.of(event)
        ));
    }

    private Map<String, Object> row() {
        return jdbcTemplate.queryForMap(
                "SELECT * FROM log_queue WHERE run_id = ?",
                runId
        );
    }

    private void assertPrepared(
            PreparedLogEvent prepared,
            int expectedAttempts
    ) {
        var stored = row();

        assertEquals("PROCESSING", stored.get("state"));
        assertNotNull(stored.get("processing_started_at_ms"));

        assertEquals(
                expectedAttempts,
                ((Number) stored.get("attempt_count")).intValue()
        );
        assertEquals(
                ((Number) stored.get("id")).longValue(),
                prepared.queueId()
        );

        assertEquals(runId, prepared.runId());
        assertEquals("source-01", prepared.sourceId());
        assertEquals(eventId(), prepared.eventId());

        assertEquals(
                prepared.result(),
                mapper.readTree((String) stored.get("result_json"))
        );
    }
}