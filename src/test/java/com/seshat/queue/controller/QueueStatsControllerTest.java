package com.seshat.queue.controller;

import com.seshat.queue.QueueStatsService;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;
import org.springframework.test.web.servlet.MockMvc;

import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

@SpringBootTest(properties = {
        "spring.datasource.url=jdbc:sqlite::memory:",
        "spring.datasource.hikari.maximum-pool-size=1",
        "spring.datasource.hikari.minimum-idle=1",
        "spring.datasource.hikari.pool-name=queue-stats-test",
        "seshat.queue.worker-enabled=false"
})
@AutoConfigureMockMvc
class QueueStatsControllerTest {

    private static final String URL = "/api/queue/stats";

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @MockitoSpyBean
    private QueueStatsService stats;

    private final String runId = "stats-test-" + UUID.randomUUID();
    private final String otherRunId = runId + "-other";

    @AfterEach
    void cleanUp() {
        jdbcTemplate.update(
                "DELETE FROM log_queue WHERE run_id IN (?, ?)",
                runId,
                otherRunId
        );
    }

    @Test
    void 실행별_상태를_집계하고_원래_데이터를_변경하지_않는다()
            throws Exception {
        for (String state : List.of(
                "PENDING",
                "PROCESSING",
                "RETRY_WAIT",
                "COMPLETED",
                "STORAGE_FAILED"
        )) {
            insert(runId, state);
        }

        insert(runId, "COMPLETED");
        insert(otherRunId, "PENDING");

        var before = rows();

        mockMvc.perform(get(URL).param("run_id", runId))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.run_id").value(runId))
                .andExpect(jsonPath("$.total_count").value(6))
                .andExpect(jsonPath("$.pending_count").value(1))
                .andExpect(jsonPath("$.processing_count").value(1))
                .andExpect(jsonPath("$.retry_wait_count").value(1))
                .andExpect(jsonPath("$.completed_count").value(2))
                .andExpect(jsonPath("$.storage_failed_count").value(1));

        assertEquals(before, rows());
    }

    @Test
    void 일치하는_이벤트가_없으면_모든_건수가_0이다()
            throws Exception {
        // 다른 실행의 이벤트는 집계에 포함되지 않는다.
        insert(otherRunId, "COMPLETED");

        mockMvc.perform(get(URL).param("run_id", runId))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.run_id").value(runId))
                .andExpect(jsonPath("$.total_count").value(0))
                .andExpect(jsonPath("$.pending_count").value(0))
                .andExpect(jsonPath("$.processing_count").value(0))
                .andExpect(jsonPath("$.retry_wait_count").value(0))
                .andExpect(jsonPath("$.completed_count").value(0))
                .andExpect(jsonPath("$.storage_failed_count").value(0));
    }

    @Test
    void 실행_ID가_없거나_공백이면_400을_반환한다()
            throws Exception {
        var requests = List.of(
                get(URL),
                get(URL).param("run_id", ""),
                get(URL).param("run_id", "   ")
        );

        for (var request : requests) {
            mockMvc.perform(request)
                    .andExpect(status().isBadRequest())
                    .andExpect(
                            jsonPath("$.code").value("INVALID_REQUEST")
                    );
        }
    }

    @Test
    void DB_조회_실패는_503을_반환한다() throws Exception {
        doThrow(new DataAccessResourceFailureException(
                "Forced query failure."
        )).when(stats).getStats(runId);

        mockMvc.perform(get(URL).param("run_id", runId))
                .andExpect(status().isServiceUnavailable())
                .andExpect(
                        jsonPath("$.code").value("STORAGE_UNAVAILABLE")
                );
    }

    private void insert(String targetRunId, String state) {
        String eventId = UUID.randomUUID().toString();

        String payload = """
                {"event_id":"%s","raw":"{}"}
                """.formatted(eventId);

        String result = """
                {"raw":"{}","parse_status":"SUCCESS","parsed":{}}
                """;

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
                    ?, 'source-01', ?, 'CENTRAL',
                    ?, ?, ?,
                    100, ?, 3, ?, ?
                )
                """,
                targetRunId,
                eventId,
                payload,
                result,
                state,
                state.equals("PROCESSING") ? 200L : null,
                state.equals("RETRY_WAIT") ? Long.MAX_VALUE : 0L,
                state.equals("COMPLETED")
                        ? null
                        : "Previous storage failure."
        );
    }

    private List<Map<String, Object>> rows() {
        return jdbcTemplate.queryForList(
                """
                SELECT *
                FROM log_queue
                WHERE run_id IN (?, ?)
                ORDER BY id
                """,
                runId,
                otherRunId
        );
    }
}