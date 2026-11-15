package com.seshat.repair;

import com.seshat.ingestion.dto.LogBatchRequest;
import com.seshat.ingestion.dto.LogEventRequest;
import com.seshat.ingestion.dto.ParsingMode;
import com.seshat.ingestion.service.LogBatchStorageService;
import com.seshat.queue.QueueProcessingService;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import tools.jackson.databind.json.JsonMapper;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static com.seshat.repair.RepairCallException.Reason.*;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest(properties = {
        "spring.datasource.url=jdbc:sqlite::memory:",
        "seshat.repair.enabled=true",
        "seshat.repair.scheduler-enabled=false",
        "seshat.repair.provider=openAiCompatibleRepair",
        "seshat.repair.http.model=test-repair-model",
        "seshat.repair.http.api-key="
})
class OpenAiCompatibleJsonRepairClientIntegrationTest {

    private static final JsonMapper JSON = JsonMapper.builder().build();
    private static final String RAW = "{\"a\":\"서비스 \\\"준비\\\"\" \"b\":2}";
    private static final String FIXED = "{\"a\":\"서비스 \\\"준비\\\"\",\"b\":2}";
    private static final String PATH = "/v1/chat/completions";
    private static final AtomicReference<Reply> REPLY = new AtomicReference<>();
    private static final AtomicReference<Request> REQUEST = new AtomicReference<>();
    private static final HttpServer SERVER = startServer();

    @Autowired private JsonRepairClient client;
    @Autowired private JsonRepairWorker worker;
    @Autowired private QueueProcessingService queue;
    @Autowired private LogBatchStorageService storage;
    @Autowired private JdbcTemplate jdbc;

    @DynamicPropertySource
    static void endpoint(DynamicPropertyRegistry properties) {
        properties.add("seshat.repair.http.endpoint", () -> endpoint());
    }

    @AfterEach
    void cleanUp() {
        jdbc.update("DELETE FROM json_repair_attempt");
        jdbc.update("DELETE FROM json_repair_job");
        jdbc.update("DELETE FROM log_queue");
        REQUEST.set(null);
        REPLY.set(null);
    }

    @AfterAll
    static void stopServer() {
        SERVER.stop(0);
    }

    @Test
    void 중앙에서_복구_대상_원문과_최초_오류를_HTTP로_보내고_후보를_검증한다() {
        REPLY.set(new Reply(200, completion("stop", candidate(FIXED)), null));
        String eventId = prepare(RAW);

        assertTrue(worker.runOnce());

        var request = REQUEST.get();
        assertEquals("POST", request.method());
        assertEquals("application/json", request.contentType());
        assertNull(request.authorization());
        var body = JSON.readTree(request.body());
        assertEquals("test-repair-model", body.path("model").stringValue());
        assertEquals("json_object", body.at("/response_format/type").stringValue());
        assertEquals("system", body.at("/messages/0/role").stringValue());
        assertEquals("user", body.at("/messages/1/role").stringValue());
        var input = JSON.readTree(body.at("/messages/1/content").stringValue());
        assertEquals(RAW, input.path("raw").stringValue());
        assertEquals("INVALID_JSON", input.path("error_code").stringValue());

        var original = JSON.readTree(jdbc.queryForObject(
                "SELECT result_json FROM log_queue WHERE event_id = ?", String.class, eventId));
        assertEquals(RAW, original.path("raw").stringValue());
        assertEquals("FAILED", original.path("parse_status").stringValue());
        assertEquals(original.at("/parse_error/message").stringValue(),
                input.path("error_message").stringValue());
        var attempt = attempt(eventId);
        assertEquals("VERIFIED", attempt.get("outcome"));
        assertEquals(FIXED, attempt.get("candidate_json"));
        assertEquals(endpoint(), attempt.get("target_id"));
        assertEquals("test-repair-model", attempt.get("model"));
        assertEquals(1, attempt.get("syntax_valid"));
        assertEquals(1, attempt.get("values_preserved"));
    }

    @Test
    void 모델의_복구_불가와_문법만_유효한_후보를_별도로_저장한다() {
        String rejected = prepare(RAW);
        REPLY.set(new Reply(200, completion("stop", candidate("{\"a\":\"다른 값\",\"b\":2}")), null));
        assertTrue(worker.runOnce());
        assertEquals("CANDIDATE_REJECTED", attempt(rejected).get("outcome"));
        assertEquals(1, attempt(rejected).get("syntax_valid"));
        assertEquals(0, attempt(rejected).get("values_preserved"));

        String unrecoverable = prepare(RAW);
        REPLY.set(new Reply(200, completion("stop",
                "{\"decision\":\"UNRECOVERABLE\",\"candidate_json\":null,\"reason\":\"모호함\"}"), null));
        assertTrue(worker.runOnce());
        assertEquals("UNRECOVERABLE", attempt(unrecoverable).get("outcome"));
        assertEquals("모호함", attempt(unrecoverable).get("reason"));
    }

    @Test
    void 비정상_응답과_HTTP_오류를_호출_실패로_구분한다() throws Exception {
        String content = candidate(FIXED);
        for (String body : List.of("", "[]", completion("length", content),
                completion("stop", content + " {}"),
                completion("stop", "{\"decision\":\"CANDIDATE\",\"decision\":\"CANDIDATE\"}"))) {
            REPLY.set(new Reply(200, body, null));
            assertNull(client.repair(request(Duration.ofSeconds(3))).decision());
        }
        String malformed = prepare(RAW);
        REPLY.set(new Reply(200, completion("stop", content + " {}"), null));
        assertTrue(worker.runOnce());
        assertEquals("INVALID_RESPONSE", attempt(malformed).get("reason"));
        assertEquals(content + " {}", attempt(malformed).get("response_text"));

        for (var entry : Map.of(401, AUTHENTICATION, 403, AUTHENTICATION, 408, TIMEOUT,
                429, SERVER_ERROR, 503, SERVER_ERROR, 400, INVALID_RESPONSE).entrySet()) {
            REPLY.set(new Reply(entry.getKey(), "error", null));
            var exception = assertThrows(RepairCallException.class,
                    () -> client.repair(request(Duration.ofSeconds(3))));
            assertEquals(entry.getValue(), exception.reason());
        }
    }

    @Test
    void 헤더가_도착한_후에도_본문_수신_제한_시간을_적용한다() {
        var sendBody = new CountDownLatch(1);
        var headersSent = new CountDownLatch(1);
        REPLY.set(new Reply(200, completion("stop", candidate(FIXED)), () -> {
            headersSent.countDown();
            try {
                sendBody.await(5, TimeUnit.SECONDS);
            } catch (InterruptedException exception) {
                Thread.currentThread().interrupt();
            }
        }));

        try {
            var exception = assertThrows(RepairCallException.class,
                    () -> client.repair(request(Duration.ofSeconds(2))));
            assertEquals(TIMEOUT, exception.reason());
            assertEquals(0, headersSent.getCount());
        } finally {
            sendBody.countDown();
        }
    }

    private String prepare(String raw) {
        UUID id = UUID.randomUUID();
        storage.store(new LogBatchRequest("http-repair-run", "test-source", ParsingMode.CENTRAL,
                List.of(new LogEventRequest(id, raw, null, null, null))));
        assertEquals(id, queue.prepareNext().orElseThrow().eventId());
        return id.toString();
    }

    private Map<String, Object> attempt(String eventId) {
        return jdbc.queryForMap("""
                SELECT attempt.*
                FROM json_repair_attempt attempt
                JOIN json_repair_job job ON job.id = attempt.job_id
                WHERE job.event_id = ?
                """, eventId);
    }

    private RepairRequest request(Duration timeout) {
        return new RepairRequest(RAW, "INVALID_JSON", "Invalid JSON syntax.", timeout);
    }

    private static String candidate(String json) {
        var decision = JSON.createObjectNode();
        decision.put("decision", "CANDIDATE");
        decision.put("candidate_json", json);
        decision.putNull("reason");
        return JSON.writeValueAsString(decision);
    }

    private static String completion(String finishReason, String content) {
        var response = JSON.createObjectNode();
        var choice = response.putArray("choices").addObject();
        choice.put("finish_reason", finishReason);
        choice.putObject("message").put("content", content);
        return JSON.writeValueAsString(response);
    }

    private static String endpoint() {
        return "http://127.0.0.1:" + SERVER.getAddress().getPort() + PATH;
    }

    private static HttpServer startServer() {
        try {
            var server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
            server.createContext(PATH, exchange -> {
                try (exchange) {
                    var headers = exchange.getRequestHeaders();
                    REQUEST.set(new Request(exchange.getRequestMethod(), headers.getFirst("Content-Type"),
                            headers.getFirst("Authorization"), new String(exchange.getRequestBody().readAllBytes(), UTF_8)));
                    Reply reply = REPLY.get();
                    byte[] bytes = reply.body().getBytes(UTF_8);
                    exchange.getResponseHeaders().set("Content-Type", "application/json");
                    exchange.sendResponseHeaders(reply.status(), bytes.length);
                    if (reply.beforeBody() != null) {
                        reply.beforeBody().run();
                    }
                    exchange.getResponseBody().write(bytes);
                }
            });
            server.start();
            return server;
        } catch (IOException exception) {
            throw new UncheckedIOException(exception);
        }
    }

    private record Reply(int status, String body, Runnable beforeBody) {
    }

    private record Request(String method, String contentType, String authorization, String body) {
    }
}
