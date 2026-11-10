package com.seshat.ingestion.controller;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.http.MediaType;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.ResultActions;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.node.ObjectNode;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

@SpringBootTest(properties = {
        "spring.datasource.url=jdbc:sqlite::memory:",
        "spring.datasource.hikari.maximum-pool-size=1",
        "spring.datasource.hikari.minimum-idle=1"
})
@AutoConfigureMockMvc
class LogBatchControllerTest {

    private static final String URL = "/api/logs/batches";

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Autowired
    private JsonMapper mapper;

    private final String runId = "http-test-" + UUID.randomUUID();

    @Test
    void 중앙_보관이_완료되면_202와_건수를_반환한다() throws Exception {
        String raw = "  {\"message\":\"안녕\"}  ";

        postBatch(request(
                event(1, raw),
                event(2, "{\"message\":")
        ))
                .andExpect(status().isAccepted())
                .andExpect(jsonPath("$.accepted_count").value(2))
                .andExpect(jsonPath("$.inserted_count").value(2))
                .andExpect(jsonPath("$.duplicate_count").value(0));

        assertEquals(2L, countRows());
        assertEquals(raw, payload(1).path("raw").stringValue());

        // 원문의 문법 오류도 중앙 모드에서는 정상 수신한다.
        assertEquals(
                "{\"message\":",
                payload(2).path("raw").stringValue()
        );
        assertFalse(payload(2).has("parsed"));
    }

    @Test
    void 동일_요청의_재전송은_202와_중복_건수를_반환한다() throws Exception {
        var body = request(event(1, "{}"));

        postBatch(body)
                .andExpect(status().isAccepted());

        postBatch(body)
                .andExpect(status().isAccepted())
                .andExpect(jsonPath("$.accepted_count").value(1))
                .andExpect(jsonPath("$.inserted_count").value(0))
                .andExpect(jsonPath("$.duplicate_count").value(1));

        assertEquals(1L, countRows());
    }

    @Test
    void HTTP_JSON_자체가_잘못되면_400을_반환한다() throws Exception {
        mockMvc.perform(post(URL)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.code").value("INVALID_REQUEST"));
    }

    @Test
    void 잘못된_요청_필드는_400을_반환하고_저장하지_않는다()
            throws Exception {

        var missingRaw = event(1, "{}");
        missingRaw.remove("raw");

        var inconsistent = event(1, "{}");
        inconsistent.put("parse_status", "SUCCESS");

        var invalidBodies = List.of(
                request(),                 // 빈 배치
                request(missingRaw),       // raw 누락
                request(inconsistent)      // CENTRAL에 파싱 상태 존재
        );

        for (var body : invalidBodies) {
            postBatch(body)
                    .andExpect(status().isBadRequest())
                    .andExpect(jsonPath("$.code").value("INVALID_REQUEST"));
        }

        assertEquals(0L, countRows());
    }

    @Test
    void ID_충돌은_409를_반환하고_배치_전체를_롤백한다()
            throws Exception {

        String originalRaw = "{\"value\":1}";

        postBatch(request(event(1, originalRaw)))
                .andExpect(status().isAccepted());

        // 신규 이벤트를 먼저 INSERT한 다음 기존 ID와 충돌시킨다.
        postBatch(request(
                event(2, "{}"),
                event(1, "{\"value\":2}")
        ))
                .andExpect(status().isConflict())
                .andExpect(jsonPath("$.code").value("EVENT_ID_CONFLICT"));

        assertEquals(1L, countRows());
        assertEquals(
                originalRaw,
                payload(1).path("raw").stringValue()
        );
    }

    @Test
    void 에이전트의_숫자와_오류가_HTTP를_거쳐도_보존된다()
            throws Exception {

        BigInteger integer = new BigInteger(
                "123456789012345678901234567890"
        );
        BigDecimal decimal = new BigDecimal(
                "0.12345678901234567890123456789"
        );

        var parsed = mapper.createObjectNode();
        parsed.put("integer", integer);
        parsed.put("decimal", decimal);

        var success = event(1, mapper.writeValueAsString(parsed));
        success.put("parse_status", "SUCCESS");
        success.set("parsed", parsed);

        var failure = event(2, "{\"message\":");
        failure.put("parse_status", "FAILED");
        failure.putObject("parse_error")
                .put("code", "INVALID_JSON")
                .put("message", "Invalid JSON syntax.");

        var body = request(success, failure);
        body.put("mode", "AGENT");

        postBatch(body)
                .andExpect(status().isAccepted())
                .andExpect(jsonPath("$.inserted_count").value(2));

        var stored = payload(1);

        assertEquals(
                integer,
                stored.at("/parsed/integer")
                        .decimalValue().toBigIntegerExact()
        );
        assertEquals(
                0,
                decimal.compareTo(
                        stored.at("/parsed/decimal").decimalValue()
                )
        );

        assertEquals(
                "INVALID_JSON",
                payload(2).at("/parse_error/code").stringValue()
        );
        assertFalse(payload(2).has("parsed"));
        assertEquals(2L, countRows());
    }

    @Test
    void DB_저장_실패는_503을_반환하고_배치_전체를_롤백한다()
            throws Exception {

        String triggerName = "fail_insert_"
                + UUID.randomUUID().toString().replace("-", "");

        // 이 테스트의 두 번째 이벤트에서만 INSERT를 실패시킨다.
        jdbcTemplate.execute("""
                CREATE TEMP TRIGGER %s
                BEFORE INSERT ON log_queue
                WHEN NEW.run_id = '%s'
                  AND NEW.event_id = '%s'
                BEGIN
                    SELECT RAISE(ABORT, 'forced storage failure');
                END;
                """.formatted(
                triggerName,
                runId,
                new UUID(0, 2)
        ));

        try {
            postBatch(request(
                    event(1, "{}"),
                    event(2, "{}")
            ))
                    .andExpect(status().isServiceUnavailable())
                    .andExpect(
                            jsonPath("$.code").value("STORAGE_UNAVAILABLE")
                    );

            assertEquals(0L, countRows());
        } finally {
            jdbcTemplate.execute(
                    "DROP TRIGGER IF EXISTS " + triggerName
            );
        }
    }

    private ObjectNode event(long sequence, String raw) {
        var event = mapper.createObjectNode();
        event.put("event_id", new UUID(0, sequence).toString());
        event.put("raw", raw);
        return event;
    }

    private ObjectNode request(ObjectNode... events) {
        var body = mapper.createObjectNode();
        body.put("run_id", runId);
        body.put("source_id", "source-01");
        body.put("mode", "CENTRAL");

        var array = body.putArray("events");

        for (var event : events) {
            array.add(event);
        }

        return body;
    }

    private ResultActions postBatch(JsonNode body) throws Exception {
        return mockMvc.perform(post(URL)
                .contentType(MediaType.APPLICATION_JSON)
                .content(mapper.writeValueAsBytes(body)));
    }

    private long countRows() {
        return jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM log_queue WHERE run_id = ?",
                Long.class,
                runId
        );
    }

    private JsonNode payload(long sequence) {
        String json = jdbcTemplate.queryForObject(
                """
                SELECT payload_json FROM log_queue
                WHERE run_id = ?
                  AND source_id = ?
                  AND event_id = ?
                """,
                String.class,
                runId,
                "source-01",
                new UUID(0, sequence).toString()
        );

        return mapper.readTree(json);
    }
}