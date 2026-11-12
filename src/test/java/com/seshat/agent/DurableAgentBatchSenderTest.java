package com.seshat.agent;

import com.seshat.ingestion.dto.LogBatchResponse;
import com.seshat.ingestion.dto.ParsingMode;
import com.seshat.processing.JavaLogProcessor;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
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
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.*;

@Timeout(30)
class DurableAgentBatchSenderTest {

    private static final String RUN_ID = "durable-sender-test";
    private static final String SOURCE_ID = "source-01";

    private final JsonMapper mapper = JsonMapper.builder().build();

    private final BlockingQueue<Received> received =
            new LinkedBlockingQueue<>();

    private final BlockingQueue<Reply> replies =
            new LinkedBlockingQueue<>();

    private final AtomicReference<Throwable> serverFailure =
            new AtomicReference<>();

    @TempDir
    Path tempDir;

    private HttpServer server;
    private Path inputFile;
    private Path queueDirectory;

    private AgentFileQueue queue;
    private DurableAgentFileSource source;
    private Thread reader;

    private DurableAgentBatchSender sender;
    private Thread senderThread;

    @BeforeEach
    void setUp() throws Exception {
        server = HttpServer.create(
                new InetSocketAddress("127.0.0.1", 0),
                0
        );

        server.createContext(
                "/api/logs/batches",
                this::handleRequest
        );

        server.start();

        inputFile = tempDir.resolve("input.ndjson");
        queueDirectory = tempDir.resolve("queue");

        Files.writeString(inputFile, "", StandardCharsets.UTF_8);

        URI endpoint = URI.create(
                "http://127.0.0.1:"
                        + server.getAddress().getPort()
                        + "/api/logs/batches"
        );

        queue = new AgentFileQueue(
                queueDirectory,
                inputFile,
                RUN_ID,
                SOURCE_ID,
                endpoint,
                10,
                16L * 1024 * 1024
        );
    }

    @AfterEach
    void tearDown() throws Exception {
        try {
            if (sender != null) {
                sender.close();
            }

            // 검증 실패로 응답 대기가 남아 있어도 서버 처리를 해제한다.
            replies.offer(new Reply(503, "{}"));

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

            if (senderThread != null) {
                assertTrue(
                        senderThread.join(Duration.ofSeconds(5)),
                        "전송 스레드가 종료되지 않았습니다."
                );
            }

            // 수집·전송 스레드가 종료된 뒤 큐를 닫는다.
            if (queue != null) {
                queue.close();
            }
        } finally {
            if (server != null) {
                server.stop(0);
            }
        }
    }

    @Test
    void CENTRAL에서_AGENT로_전환해도_기존_배치는_같은_본문으로_재시도한다()
            throws Exception {

        verifyRetryDuringTransition(
                ParsingMode.CENTRAL,
                ParsingMode.AGENT
        );
    }

    @Test
    void AGENT에서_CENTRAL로_전환해도_기존_배치는_같은_본문으로_재시도한다()
            throws Exception {

        verifyRetryDuringTransition(
                ParsingMode.AGENT,
                ParsingMode.CENTRAL
        );
    }

    @Test
    void 잘못된_JSON의_202_응답은_확인으로_인정하지_않는다()
            throws Exception {

        verifyInvalidAcknowledgement("{");
    }

    @Test
    void 저장_건수의_합이_맞지_않는_202_응답은_확인으로_인정하지_않는다()
            throws Exception {

        verifyInvalidAcknowledgement(
                mapper.writeValueAsString(
                        new LogBatchResponse(1, 0, 0)
                )
        );
    }

    @Test
    void HTTP_409는_오류로_전달하고_배치_파일을_유지한다()
            throws Exception {

        var state = new AgentModeState(ParsingMode.CENTRAL);

        readOneLine(state, "{\"message\":\"거절 확인\"}");

        Path storedFile = onlyBatchFile();
        byte[] storedPacket = Files.readAllBytes(storedFile);

        startSender();

        var request = nextRequest();

        assertBatch(request, ParsingMode.CENTRAL);
        assertStored(storedFile, storedPacket);

        reply(409, "{}");

        var error = assertThrows(
                ExecutionException.class,
                () -> sender.completion()
                        .toCompletableFuture()
                        .get(10, TimeUnit.SECONDS)
        );

        var cause = assertInstanceOf(
                IllegalStateException.class,
                error.getCause()
        );

        assertTrue(cause.getMessage().contains("409"));

        assertTrue(
                senderThread.join(Duration.ofSeconds(2)),
                "오류 전달 후 전송 스레드가 종료되지 않았습니다."
        );

        assertEquals(1, queue.pendingBatchCount());
        assertStored(storedFile, storedPacket);
        assertTrue(received.isEmpty());
        assertNull(serverFailure.get());
    }

    private void verifyRetryDuringTransition(
            ParsingMode initialMode,
            ParsingMode nextMode
    ) throws Exception {

        var state = new AgentModeState(initialMode);

        readOneLine(state, "{\"message\":\"기존 배치\"}");

        // 전송기를 시작하기 전에만 큐에서 검증용 본문을 읽는다.
        byte[] originalBody = queue.take().body();

        Path oldFile = onlyBatchFile();
        byte[] originalPacket = Files.readAllBytes(oldFile);

        startSender();

        var first = nextRequest();

        assertBatch(first, initialMode);
        assertArrayEquals(originalBody, first.bytes());
        assertEquals(1, queue.pendingBatchCount());
        assertStored(oldFile, originalPacket);

        // 첫 요청의 응답을 보류한 동안 실제 수집기에 새 모드를 적용한다.
        state.requestMode(nextMode);

        readOneLine(state, "{\"message\":\"새 모드 배치\"}");

        assertEquals(nextMode, state.snapshot().appliedMode());
        assertFalse(state.snapshot().hasPendingRequest());
        assertEquals(2, queue.pendingBatchCount());
        assertEquals(2, batchFiles().size());
        assertStored(oldFile, originalPacket);

        reply(503, "{}");

        var retry = nextRequest();

        assertBatch(retry, initialMode);
        assertArrayEquals(first.bytes(), retry.bytes());
        assertEquals(
                first.body().path("events").get(0).path("event_id").asText(),
                retry.body().path("events").get(0).path("event_id").asText()
        );

        // 재시도 요청도 아직 확인받지 않았으므로 두 배치가 모두 남는다.
        assertEquals(2, queue.pendingBatchCount());
        assertStored(oldFile, originalPacket);

        acknowledge(false);

        var next = nextRequest();

        // 기존 배치 확인 처리 후에야 새 모드 배치를 전송한다.
        assertBatch(next, nextMode);
        var nextEvent = next.body().path("events").get(0);

        assertEquals(
                "{\"message\":\"새 모드 배치\"}",
                nextEvent.path("raw").asText()
        );


        assertEquals(1, queue.pendingBatchCount());
        assertFalse(Files.exists(oldFile));
        assertEquals(1, batchFiles().size());

        acknowledge(false);

        await(() -> queue.pendingBatchCount() == 0);

        assertTrue(batchFiles().isEmpty());
        assertEquals(Files.size(inputFile), queue.checkpoint().offset());

        stopSender();

        assertTrue(received.isEmpty());
        assertNull(serverFailure.get());
    }

    private void verifyInvalidAcknowledgement(String invalidBody)
            throws Exception {

        var state = new AgentModeState(ParsingMode.AGENT);

        readOneLine(state, "{\"message\":\"잘못된 확인 응답\"}");

        Path storedFile = onlyBatchFile();
        byte[] storedPacket = Files.readAllBytes(storedFile);

        startSender();

        var first = nextRequest();

        assertBatch(first, ParsingMode.AGENT);

        reply(202, invalidBody);

        var retry = nextRequest();

        assertArrayEquals(first.bytes(), retry.bytes());
        assertEquals(1, queue.pendingBatchCount());
        assertStored(storedFile, storedPacket);

        // 중앙에 이미 저장됐다는 중복 확인 응답도 유효한 확인이다.
        acknowledge(true);

        await(() -> queue.pendingBatchCount() == 0);

        assertFalse(Files.exists(storedFile));
        assertTrue(batchFiles().isEmpty());

        stopSender();

        assertTrue(received.isEmpty());
        assertNull(serverFailure.get());
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
                .name("durable-sender-reader-test")
                .daemon(true)
                .unstarted(source);

        reader.start();

        long deadline = System.nanoTime()
                + Duration.ofSeconds(5).toNanos();

        while (queue.pendingBatchCount() < expectedBatches) {
            if (source.completion().toCompletableFuture().isDone()) {
                source.completion().toCompletableFuture().get();

                fail("배치 보관 전에 수집기가 종료되었습니다.");
            }

            if (System.nanoTime() - deadline >= 0) {
                fail("입력이 파일 큐에 보관되지 않았습니다.");
            }

            Thread.sleep(5);
        }

        source.close();

        source.completion()
                .toCompletableFuture()
                .get(5, TimeUnit.SECONDS);

        assertTrue(
                reader.join(Duration.ofSeconds(2)),
                "수집 스레드가 종료되지 않았습니다."
        );

        assertEquals(expectedBatches, queue.pendingBatchCount());
    }

    private void startSender() {
        sender = new DurableAgentBatchSender(queue);

        senderThread = Thread.ofPlatform()
                .name("durable-batch-sender-test")
                .daemon(true)
                .unstarted(sender);

        senderThread.start();
    }

    private void stopSender() throws Exception {
        sender.close();

        sender.completion()
                .toCompletableFuture()
                .get(5, TimeUnit.SECONDS);

        assertTrue(
                senderThread.join(Duration.ofSeconds(2)),
                "전송 스레드가 종료되지 않았습니다."
        );
    }

    private void handleRequest(HttpExchange exchange)
            throws IOException {

        try {
            byte[] bytes = exchange.getRequestBody().readAllBytes();

            received.add(new Received(
                    bytes,
                    mapper.readTree(bytes)
            ));

            // 검증 코드가 응답을 지정할 때까지 보관 확인을 보내지 않는다.
            Reply response = replies.poll(8, TimeUnit.SECONDS);

            if (response == null) {
                serverFailure.compareAndSet(
                        null,
                        new TimeoutException(
                                "테스트의 HTTP 응답 지정 시간이 초과됐습니다."
                        )
                );

                response = new Reply(503, "{}");
            }

            byte[] body = response.body()
                    .getBytes(StandardCharsets.UTF_8);

            exchange.getResponseHeaders().set(
                    "Content-Type",
                    "application/json"
            );

            exchange.sendResponseHeaders(
                    response.status(),
                    body.length
            );

            exchange.getResponseBody().write(body);

        } catch (InterruptedException error) {
            Thread.currentThread().interrupt();

        } catch (IOException | RuntimeException error) {
            serverFailure.compareAndSet(null, error);
            throw error;

        } finally {
            exchange.close();
        }
    }

    private Received nextRequest() throws InterruptedException {
        var request = received.poll(8, TimeUnit.SECONDS);

        assertNull(
                serverFailure.get(),
                "테스트 HTTP 서버에서 오류가 발생했습니다."
        );

        assertNotNull(
                request,
                "제한 시간 안에 배치 요청이 도착하지 않았습니다."
        );

        return request;
    }

    private void reply(int status, String body) {
        replies.add(new Reply(status, body));
    }

    private void acknowledge(boolean duplicate) {
        reply(
                202,
                mapper.writeValueAsString(
                        new LogBatchResponse(
                                1,
                                duplicate ? 0 : 1,
                                duplicate ? 1 : 0
                        )
                )
        );
    }

    private void assertBatch(
            Received request,
            ParsingMode expectedMode
    ) {
        assertEquals(RUN_ID, request.body().path("run_id").asText());
        assertEquals(SOURCE_ID, request.body().path("source_id").asText());
        assertEquals(
                expectedMode.name(),
                request.body().path("mode").asText()
        );
        assertEquals(1, request.body().path("events").size());

        var event = request.body().path("events").get(0);

        if (expectedMode == ParsingMode.AGENT) {
            assertEquals(
                    "SUCCESS",
                    event.path("parse_status").asText()
            );
            assertEquals(
                    mapper.readTree(event.path("raw").asText()),
                    event.get("parsed")
            );
            assertFalse(event.has("parse_error"));
        } else {
            assertFalse(event.has("parse_status"));
            assertFalse(event.has("parsed"));
            assertFalse(event.has("parse_error"));
        }
    }

    private void assertStored(
            Path path,
            byte[] expectedPacket
    ) throws IOException {

        assertTrue(
                Files.exists(path),
                "중앙 확인 전에 배치 파일이 삭제됐습니다."
        );

        assertArrayEquals(
                expectedPacket,
                Files.readAllBytes(path)
        );
    }

    private List<Path> batchFiles() throws IOException {
        try (var files = Files.list(queueDirectory)) {
            return files
                    .filter(path -> path.getFileName()
                            .toString().endsWith(".batch"))
                    .sorted()
                    .toList();
        }
    }

    private Path onlyBatchFile() throws IOException {
        var files = batchFiles();

        assertEquals(1, files.size());

        return files.get(0);
    }

    private void await(BooleanSupplier condition) throws Exception {
        long deadline = System.nanoTime()
                + Duration.ofSeconds(5).toNanos();

        while (!condition.getAsBoolean()) {
            assertNull(
                    serverFailure.get(),
                    "테스트 HTTP 서버에서 오류가 발생했습니다."
            );

            if (sender.completion().toCompletableFuture().isDone()) {
                sender.completion().toCompletableFuture().get();

                fail("확인 처리가 끝나기 전에 전송기가 종료됐습니다.");
            }

            if (System.nanoTime() - deadline >= 0) {
                fail("제한 시간 안에 배치 확인 처리가 끝나지 않았습니다.");
            }

            Thread.sleep(5);
        }
    }

    private record Received(
            byte[] bytes,
            JsonNode body
    ) {
    }

    private record Reply(
            int status,
            String body
    ) {
    }
}