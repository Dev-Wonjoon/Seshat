package com.seshat.agent;

import com.seshat.ingestion.dto.ParsingMode;
import com.seshat.processing.JavaLogProcessor;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.api.parallel.Resources;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

@Timeout(30)
@Execution(ExecutionMode.SAME_THREAD)
@ResourceLock(Resources.SYSTEM_PROPERTIES)
class AgentHeartbeatReporterTest {

    private static final String RUN_ID = "heartbeat-mode-test";
    private static final String SOURCE_ID = "source-01";

    private static final int MAX_PENDING_BATCHES = 10;
    private static final long MAX_PENDING_BYTES = 16L * 1024 * 1024;

    private static final String INTERVAL_PROPERTY =
            "seshat.agent.heartbeat-interval-ms";

    private static final String ENDPOINT_PROPERTY =
            "seshat.agent.heartbeat-endpoint";

    private final JsonMapper mapper = JsonMapper.builder().build();

    private final BlockingQueue<Received> received =
            new LinkedBlockingQueue<>();

    private final AtomicInteger responseStatus = new AtomicInteger(204);

    @TempDir
    Path tempDir;

    private String previousInterval;
    private String previousEndpoint;

    private HttpServer server;
    private Path inputFile;
    private AgentFileQueue queue;
    private AgentHeartbeatReporter heartbeat;
    private DurableAgentFileSource source;
    private Thread reader;

    @BeforeEach
    void setUp() throws Exception {
        previousInterval = System.getProperty(INTERVAL_PROPERTY);
        previousEndpoint = System.getProperty(ENDPOINT_PROPERTY);

        server = HttpServer.create(
                new InetSocketAddress("127.0.0.1", 0),
                0
        );

        server.createContext(
                "/api/agents/heartbeat",
                this::handleHeartbeat
        );

        server.start();

        String baseUrl =
                "http://127.0.0.1:" + server.getAddress().getPort();

        System.setProperty(INTERVAL_PROPERTY, "100");
        System.setProperty(
                ENDPOINT_PROPERTY,
                baseUrl + "/api/agents/heartbeat"
        );

        inputFile = tempDir.resolve("input.ndjson");

        Files.writeString(
                inputFile,
                "",
                StandardCharsets.UTF_8
        );

        queue = new AgentFileQueue(
                tempDir.resolve("queue"),
                inputFile,
                RUN_ID,
                SOURCE_ID,
                URI.create(baseUrl + "/api/logs/batches"),
                MAX_PENDING_BATCHES,
                MAX_PENDING_BYTES
        );
    }

    @AfterEach
    void tearDown() throws Exception {
        try {
            if (heartbeat != null) {
                heartbeat.close();
            }

            if (source != null) {
                source.close();
            }

            if (reader != null
                    && !reader.join(Duration.ofSeconds(1))) {

                reader.interrupt();

                assertTrue(
                        reader.join(Duration.ofSeconds(3)),
                        "수집 스레드가 종료되지 않았습니다."
                );
            }

            // 수집 스레드가 종료된 뒤 큐를 닫는다.
            if (queue != null) {
                queue.close();
            }
        } finally {
            if (server != null) {
                server.stop(0);
            }

            restoreProperty(INTERVAL_PROPERTY, previousInterval);
            restoreProperty(ENDPOINT_PROPERTY, previousEndpoint);
        }
    }

    @Test
    void CENTRAL에서_AGENT로_요청만으로는_보고가_바뀌지_않고_실제_수집후_바뀐다()
            throws Exception {

        verifyAppliedReport(
                ParsingMode.CENTRAL,
                ParsingMode.AGENT
        );
    }

    @Test
    void AGENT에서_CENTRAL로_요청만으로는_보고가_바뀌지_않고_실제_수집후_바뀐다()
            throws Exception {

        verifyAppliedReport(
                ParsingMode.AGENT,
                ParsingMode.CENTRAL
        );
    }

    @Test
    void 과거_모드의_배치가_남아_있어도_현재_적용_모드를_보고한다()
            throws Exception {

        var state = new AgentModeState(ParsingMode.CENTRAL);

        readOneLine(state, "{\"message\":\"기존 배치\"}");

        var oldBatch = queue.take();
        byte[] oldBody = oldBatch.body();

        startHeartbeat(state);

        var initial = nextHeartbeat();

        assertMode(initial, ParsingMode.CENTRAL);
        assertEquals(
                1,
                initial.body().path("pending_batch_count").intValue()
        );

        state.requestMode(ParsingMode.AGENT);

        readOneLine(state, "{\"message\":\"새 배치\"}");

        var applied = freshHeartbeat();

        assertMode(applied, ParsingMode.AGENT);
        assertEquals(
                2,
                applied.body().path("pending_batch_count").intValue()
        );

        // 가장 오래된 배치는 여전히 CENTRAL이며 본문도 그대로다.
        var oldest = queue.take();

        assertArrayEquals(oldBody, oldest.body());
        assertEquals(
                "CENTRAL",
                mapper.readTree(oldest.body()).path("mode").asText()
        );
        assertFalse(state.snapshot().hasPendingRequest());
    }

    @Test
    void 보고가_503이어도_수집을_계속하고_복구후_최신_적용_모드를_보고한다()
            throws Exception {

        var state = new AgentModeState(ParsingMode.CENTRAL);

        startHeartbeat(state);

        var initial = nextHeartbeat();

        assertEquals(204, initial.status());
        assertMode(initial, ParsingMode.CENTRAL);

        responseStatus.set(503);

        var failed = freshHeartbeat();

        assertEquals(503, failed.status());
        assertMode(failed, ParsingMode.CENTRAL);

        state.requestMode(ParsingMode.AGENT);

        // 하트비트 실패 중에도 실제 입력을 파싱하고 파일 큐에 저장한다.
        readOneLine(state, "{\"message\":\"보고 장애 중 수집\"}");

        var appliedDuringFailure = freshHeartbeat();

        assertEquals(503, appliedDuringFailure.status());
        assertMode(appliedDuringFailure, ParsingMode.AGENT);
        assertEquals(1, queue.pendingBatchCount());

        responseStatus.set(204);

        var recovered = freshHeartbeat();

        assertEquals(204, recovered.status());
        assertMode(recovered, ParsingMode.AGENT);
        assertEquals(
                initial.body().path("instance_id").asText(),
                recovered.body().path("instance_id").asText()
        );
        assertFalse(state.snapshot().hasPendingRequest());
    }

    private void verifyAppliedReport(
            ParsingMode initialMode,
            ParsingMode nextMode
    ) throws Exception {

        var state = new AgentModeState(initialMode);

        startHeartbeat(state);

        var initial = nextHeartbeat();

        assertEquals(204, initial.status());
        assertMode(initial, initialMode);

        state.requestMode(nextMode);

        // 입력이 없으므로 요청은 대기하고 실제 적용 보고는 유지한다.
        var pending = freshHeartbeat();

        assertMode(pending, initialMode);
        assertTrue(state.snapshot().hasPendingRequest());
        assertEquals(0, queue.pendingBatchCount());

        readOneLine(state, "{\"message\":\"전환 확인\"}");

        var applied = freshHeartbeat();

        assertEquals(204, applied.status());
        assertMode(applied, nextMode);
        assertEquals(
                initial.body().path("instance_id").asText(),
                applied.body().path("instance_id").asText()
        );
        assertEquals(
                1,
                applied.body().path("pending_batch_count").intValue()
        );
        assertFalse(state.snapshot().hasPendingRequest());

        // 보고된 적용 모드와 실제 저장된 배치의 모드를 함께 확인한다.
        var stored = mapper.readTree(queue.take().body());

        assertEquals(nextMode.name(), stored.path("mode").asText());

        var event = stored.path("events").get(0);

        if (nextMode == ParsingMode.AGENT) {
            assertEquals(
                    "SUCCESS",
                    event.path("parse_status").asText()
            );
            assertEquals(
                    "전환 확인",
                    event.path("parsed").path("message").asText()
            );
        } else {
            assertFalse(event.has("parse_status"));
            assertFalse(event.has("parsed"));
            assertFalse(event.has("parse_error"));
        }
    }

    private void startHeartbeat(AgentModeState state) {
        heartbeat = new AgentHeartbeatReporter(
                queue,
                state,
                MAX_PENDING_BATCHES,
                MAX_PENDING_BYTES
        );

        heartbeat.start();
    }

    private void readOneLine(
            AgentModeState state,
            String raw
    ) throws Exception {

        int expectedBatches = queue.pendingBatchCount() + 1;

        Files.writeString(
                inputFile,
                raw + "\n",
                StandardCharsets.UTF_8,
                StandardOpenOption.APPEND
        );

        source = new DurableAgentFileSource(
                queue,
                new AgentEventFactory(new JavaLogProcessor()),
                state
        );

        reader = Thread.ofPlatform()
                .name("heartbeat-source-test")
                .daemon(true)
                .unstarted(source);

        reader.start();

        long deadline = System.nanoTime()
                + Duration.ofSeconds(10).toNanos();

        while (queue.pendingBatchCount() < expectedBatches) {
            if (source.completion().toCompletableFuture().isDone()) {
                source.completion().toCompletableFuture().get();

                fail("배치를 보관하기 전에 수집기가 종료되었습니다.");
            }

            if (System.nanoTime() - deadline >= 0) {
                fail("입력이 파일 큐에 보관되지 않았습니다.");
            }

            Thread.sleep(5);
        }

        source.close();

        source.completion()
                .toCompletableFuture()
                .get(10, TimeUnit.SECONDS);

        assertTrue(
                reader.join(Duration.ofSeconds(2)),
                "완료 통지 후 수집 스레드가 종료되지 않았습니다."
        );

        assertEquals(expectedBatches, queue.pendingBatchCount());
    }

    private void handleHeartbeat(HttpExchange exchange)
            throws IOException {

        try {
            var body = mapper.readTree(
                    exchange.getRequestBody().readAllBytes()
            );

            int status = responseStatus.get();

            exchange.sendResponseHeaders(status, -1);

            received.add(new Received(body, status));
        } finally {
            exchange.close();
        }
    }

    private Received nextHeartbeat() throws InterruptedException {
        var result = received.poll(10, TimeUnit.SECONDS);

        assertNotNull(
                result,
                "제한 시간 안에 하트비트가 도착하지 않았습니다."
        );

        return result;
    }

    private Received freshHeartbeat() throws InterruptedException {
        received.clear();

        // 이미 준비되어 전송 중이던 요청 한 건은 건너뛴다.
        // 보고기는 동기 전송하므로 그다음 요청은 현재 상태를 반영한다.
        nextHeartbeat();

        return nextHeartbeat();
    }

    private void assertMode(
            Received report,
            ParsingMode expectedMode
    ) {
        assertEquals(RUN_ID, report.body().path("run_id").asText());
        assertEquals(SOURCE_ID, report.body().path("source_id").asText());
        assertEquals(
                expectedMode.name(),
                report.body().path("applied_mode").asText()
        );

        assertFalse(
                report.body().path("instance_id").asText().isBlank()
        );
    }

    private void restoreProperty(String name, String previousValue) {
        if (previousValue == null) {
            System.clearProperty(name);
        } else {
            System.setProperty(name, previousValue);
        }
    }

    private record Received(
            JsonNode body,
            int status
    ) {
    }
}