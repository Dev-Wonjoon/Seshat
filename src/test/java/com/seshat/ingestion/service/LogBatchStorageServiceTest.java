package com.seshat.ingestion.service;

import com.seshat.ingestion.dto.LogBatchRequest;
import com.seshat.ingestion.dto.LogEventRequest;
import com.seshat.ingestion.dto.LogFormat;
import com.seshat.ingestion.dto.ParseError;
import com.seshat.ingestion.dto.ParseStatus;
import com.seshat.ingestion.dto.ParsingMode;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest(properties = {
        "spring.datasource.url=jdbc:sqlite::memory:",
        "spring.datasource.hikari.maximum-pool-size=1",
        "spring.datasource.hikari.minimum-idle=1"
})
class LogBatchStorageServiceTest {

    @Autowired
    private LogBatchStorageService service;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Autowired
    private JsonMapper mapper;

    // 각 테스트가 서로 다른 실행 ID를 사용한다.
    private final String runId = "test-" + UUID.randomUUID();

    @Test
    void 신규_배치를_저장하고_원문을_보존한다() {
        String raw = "  {\"message\":\"안녕\"}  ";

        var first = central(1, raw);
        var second = central(2, "{\"message\":");

        var result = service.store(
                batch(ParsingMode.CENTRAL, first, second)
        );

        assertEquals(new BatchStoreResult(2, 2, 0), result);
        assertEquals(2L, countRows());

        assertEquals(
                raw,
                payload(first.eventId()).path("raw").stringValue()
        );
        assertEquals(
                "{\"message\":",
                payload(second.eventId()).path("raw").stringValue()
        );

        assertEquals("PENDING", state(first.eventId()));
        assertEquals("PENDING", state(second.eventId()));
        assertFalse(payload(first.eventId()).has("parsed"));
    }

    @Test
    void 같은_배치를_재전송해도_추가하거나_완료_상태를_되돌리지_않는다() {
        var first = central(1, "{}");
        var second = central(2, "{\"message\":\"ok\"}");
        var request = batch(ParsingMode.CENTRAL, first, second);

        service.store(request);

        jdbcTemplate.update(
                "UPDATE log_queue SET state = 'COMPLETED' WHERE run_id = ?",
                runId
        );

        var result = service.store(request);

        assertEquals(new BatchStoreResult(2, 0, 2), result);
        assertEquals(2L, countRows());
        assertEquals("COMPLETED", state(first.eventId()));
        assertEquals("COMPLETED", state(second.eventId()));
    }

    @Test
    void 기존_이벤트와_신규_이벤트가_섞이면_신규만_추가한다() {
        var existing = central(1, "{}");
        var fresh = central(2, "{\"message\":\"new\"}");

        service.store(batch(ParsingMode.CENTRAL, existing));

        var result = service.store(
                batch(ParsingMode.CENTRAL, existing, fresh)
        );

        assertEquals(new BatchStoreResult(2, 1, 1), result);
        assertEquals(2L, countRows());
        assertEquals(
                fresh.raw(),
                payload(fresh.eventId()).path("raw").stringValue()
        );
    }

    @Test
    void 배치_중간에_내용_충돌이_나면_앞서_추가한_이벤트도_롤백한다() {
        var original = central(1, "{\"value\":1}");
        service.store(batch(ParsingMode.CENTRAL, original));

        var fresh = central(2, "{}");
        var conflict = central(1, "{\"value\":2}");

        // 신규 이벤트를 먼저 INSERT한 뒤 충돌하도록 순서를 지정한다.
        assertThrows(
                EventIdConflictException.class,
                () -> service.store(
                        batch(ParsingMode.CENTRAL, fresh, conflict)
                )
        );

        assertEquals(1L, countRows());
        assertEquals(
                original.raw(),
                payload(original.eventId()).path("raw").stringValue()
        );
    }

    @Test
    void 같은_ID의_모드가_달라지면_충돌로_처리한다() {
        var original = central(1, "{}");
        service.store(batch(ParsingMode.CENTRAL, original));

        var changed = new LogEventRequest(
                original.eventId(),
                original.raw(),
                ParseStatus.SUCCESS,
                mapper.createObjectNode(),
                null
        );

        assertThrows(
                EventIdConflictException.class,
                () -> service.store(batch(ParsingMode.AGENT, changed))
        );

        assertEquals(1L, countRows());

        String storedMode = jdbcTemplate.queryForObject(
                """
                SELECT mode FROM log_queue
                WHERE run_id = ? AND event_id = ?
                """,
                String.class,
                runId,
                original.eventId().toString()
        );

        assertEquals("CENTRAL", storedMode);
    }

    @Test
    void 에이전트의_숫자_정밀도와_파싱_오류를_보존한다() {
        BigInteger integer = new BigInteger(
                "123456789012345678901234567890"
        );
        BigDecimal decimal = new BigDecimal(
                "0.12345678901234567890123456789"
        );

        String raw = "{\"integer\":" + integer
                + ",\"decimal\":" + decimal.toPlainString() + "}";

        var success = new LogEventRequest(
                new UUID(0, 1),
                raw,
                ParseStatus.SUCCESS,
                mapper.readTree(raw),
                null
        );

        var failure = new LogEventRequest(
                new UUID(0, 2),
                "{\"message\":",
                ParseStatus.FAILED,
                null,
                new ParseError("INVALID_JSON", "Invalid JSON syntax.")
        );

        var result = service.store(
                batch(ParsingMode.AGENT, success, failure)
        );

        assertEquals(new BatchStoreResult(2, 2, 0), result);

        var storedSuccess = payload(success.eventId());

        assertEquals(
                integer,
                storedSuccess.at("/parsed/integer")
                        .decimalValue().toBigIntegerExact()
        );
        assertEquals(
                0,
                decimal.compareTo(
                        storedSuccess.at("/parsed/decimal").decimalValue()
                )
        );

        var storedFailure = payload(failure.eventId());

        assertEquals(
                "FAILED",
                storedFailure.path("parse_status").stringValue()
        );
        assertEquals(
                "INVALID_JSON",
                storedFailure.at("/parse_error/code").stringValue()
        );
        assertEquals(
                failure.raw(),
                storedFailure.path("raw").stringValue()
        );
        assertFalse(storedFailure.has("parsed"));

        // DB에서 읽은 숫자 결과로 재전송해도 중복으로 인정되어야 한다.
        var replay = new LogEventRequest(
                success.eventId(),
                success.raw(),
                ParseStatus.SUCCESS,
                storedSuccess.path("parsed"),
                null
        );

        assertEquals(
                new BatchStoreResult(1, 0, 1),
                service.store(batch(ParsingMode.AGENT, replay))
        );
        assertEquals(2L, countRows());
    }

    @Test
    void 큐는_입력_형식을_보존하고_재전송에_같은_형식을_요구한다() {
        String raw = "[23:52:17] [Server thread/INFO]: 저장 완료";
        var id = new UUID(0, 1);
        var event = new LogEventRequest(id, raw, null, null, null, LogFormat.MINECRAFT);
        assertEquals(new BatchStoreResult(1, 1, 0), service.store(batch(ParsingMode.CENTRAL, event)));
        assertEquals("MINECRAFT", payload(id).path("log_format").stringValue());
        assertEquals(new BatchStoreResult(1, 0, 1), service.store(batch(ParsingMode.CENTRAL, event)));
        var different = new LogEventRequest(id, raw, null, null, null, LogFormat.AUTO);
        assertThrows(EventIdConflictException.class, () -> service.store(batch(ParsingMode.CENTRAL, different)));
    }

    private LogEventRequest central(long sequence, String raw) {
        return new LogEventRequest(
                new UUID(0, sequence),
                raw,
                null,
                null,
                null
        );
    }

    private LogBatchRequest batch(
            ParsingMode mode,
            LogEventRequest... events
    ) {
        return new LogBatchRequest(
                runId,
                "source-01",
                mode,
                List.of(events)
        );
    }

    private long countRows() {
        return jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM log_queue WHERE run_id = ?",
                Long.class,
                runId
        );
    }

    private JsonNode payload(UUID eventId) {
        String json = jdbcTemplate.queryForObject(
                """
                SELECT payload_json FROM log_queue
                WHERE run_id = ? AND event_id = ?
                """,
                String.class,
                runId,
                eventId.toString()
        );

        return mapper.readTree(json);
    }

    private String state(UUID eventId) {
        return jdbcTemplate.queryForObject(
                """
                SELECT state FROM log_queue
                WHERE run_id = ? AND event_id = ?
                """,
                String.class,
                runId,
                eventId.toString()
        );
    }
}
