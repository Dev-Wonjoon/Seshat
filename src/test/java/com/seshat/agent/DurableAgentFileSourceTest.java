package com.seshat.agent;

import com.seshat.ingestion.dto.LogEventRequest;
import com.seshat.ingestion.dto.ParsingMode;
import com.seshat.processing.JavaLogProcessor;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.URI;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.Properties;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@Timeout(30)
class DurableAgentFileSourceTest {

    private static final String RUN_ID = "mode-source-test";
    private static final String SOURCE_ID = "source-01";
    private static final String BROKEN = "{\"message\":";

    private static final int MAX_REQUEST_BYTES = 1_048_576;
    private static final long MAX_QUEUE_BYTES = 16L * 1024 * 1024;

    // 이 테스트에서는 전송기를 실행하지 않으므로 실제 연결하지 않는다.
    private static final URI ENDPOINT = URI.create(
            "http://127.0.0.1:18080/api/logs/batches"
    );

    private final JsonMapper mapper = JsonMapper.builder().build();

    @TempDir
    Path tempDir;

    private Path inputFile;
    private AgentFileQueue queue;
    private DurableAgentFileSource source;
    private Thread reader;

    @AfterEach
    void tearDown() throws Exception {
        if (source != null) {
            source.close();
        }

        if (reader != null
                && !reader.join(Duration.ofSeconds(1))) {

            // 실패한 테스트에서 큐 용량 대기가 남아 있어도 정리한다.
            reader.interrupt();

            assertTrue(
                    reader.join(Duration.ofSeconds(3)),
                    "수집 스레드가 종료되지 않았습니다."
            );
        }

        // 수집 스레드가 종료된 뒤 파일 큐를 닫는다.
        if (queue != null) {
            queue.close();
        }
    }

    @Test
    void CRLF를_제외한_4MiB_원문은_보존하고_1바이트_초과는_거절한다()
            throws Exception {
        int lineLimit = 4 * 1024 * 1024;
        int messageLength = lineLimit - largeRaw(0).length();
        String accepted = largeRaw(messageLength);
        String oversized = largeRaw(messageLength + 1);
        assertEquals(lineLimit, accepted.getBytes(StandardCharsets.UTF_8).length);

        inputFile = tempDir.resolve("input.ndjson");
        Files.writeString(inputFile, accepted + "\r\n" + oversized + "\n",
                StandardCharsets.UTF_8);
        queue = openQueue(10);
        source = new DurableAgentFileSource(
                queue, new AgentEventFactory(new JavaLogProcessor()),
                new AgentModeState(ParsingMode.AGENT), () -> 0L
        );
        reader = Thread.ofPlatform().daemon(true).start(source);

        var error = assertThrows(ExecutionException.class, this::awaitSource);
        var cause = assertInstanceOf(IllegalArgumentException.class, error.getCause());
        assertEquals("Input line exceeds the 4 MiB limit.", cause.getMessage());
        assertEquals(1, queue.pendingBatchCount());
        assertEquals(lineLimit + 2L, queue.checkpoint().offset());

        var events = mapper.readTree(takeBatch().body()).path("events");
        assertEquals(1, events.size());
        assertEquals("SUCCESS", events.get(0).path("parse_status").asText());
        assertEquals(accepted, events.get(0).path("raw").asText());
    }

    @Test
    void CENTRAL에서_AGENT로_전환해도_구성중인_배치의_모드는_유지한다()
            throws Exception {

        verifyBatchTransition(
                ParsingMode.CENTRAL,
                ParsingMode.AGENT
        );
    }

    @Test
    void AGENT에서_CENTRAL로_전환해도_구성중인_배치의_모드는_유지한다()
            throws Exception {

        verifyBatchTransition(
                ParsingMode.AGENT,
                ParsingMode.CENTRAL
        );
    }

    @Test
    void 크기_분할시_CENTRAL에서_AGENT로_전환하고_이벤트_ID를_유지한다()
            throws Exception {

        verifySizeSplit(
                ParsingMode.CENTRAL,
                ParsingMode.AGENT
        );
    }

    @Test
    void 크기_분할시_AGENT에서_CENTRAL로_전환하고_이벤트_ID를_유지한다()
            throws Exception {

        verifySizeSplit(
                ParsingMode.AGENT,
                ParsingMode.CENTRAL
        );
    }

    @Test
    void 새_모드에서_단일_이벤트가_초과하면_적용과_체크포인트를_진행하지_않는다()
            throws Exception {

        var state = new AgentModeState(ParsingMode.CENTRAL);

        String firstRaw = largeRaw(900_000);
        String secondRaw = largeRaw(600_000);

        var factory = scriptedFactory((number, event) -> {
            if (number == 2) {
                state.requestMode(ParsingMode.AGENT);
                source.close();
            }
        });

        start(
                List.of(firstRaw, secondRaw),
                state,
                10,
                factory
        );

        var error = assertThrows(
                ExecutionException.class,
                () -> source.completion()
                        .toCompletableFuture()
                        .get(15, TimeUnit.SECONDS)
        );

        var cause = assertInstanceOf(
                IllegalArgumentException.class,
                error.getCause()
        );

        assertTrue(cause.getMessage().contains("단일 이벤트"));
        assertTrue(reader.join(Duration.ofSeconds(2)));

        // 앞선 정상 배치만 보관하고 실패한 입력 위치는 확정하지 않는다.
        assertEquals(1, queue.pendingBatchCount());
        assertEquals(
                firstRaw.getBytes(StandardCharsets.UTF_8).length + 1L,
                queue.checkpoint().offset()
        );

        var body = inspect(takeBatch(), ParsingMode.CENTRAL);

        assertEquals(1, body.path("events").size());
        assertEquals(
                firstRaw,
                body.path("events").get(0).path("raw").asText()
        );

        var snapshot = state.snapshot();

        assertEquals(ParsingMode.AGENT, snapshot.requestedMode());
        assertEquals(ParsingMode.CENTRAL, snapshot.appliedMode());
        assertEquals(0L, snapshot.appliedRevision());
        assertTrue(snapshot.hasPendingRequest());
    }

    @Test
    void 큐_용량_대기중_요청은_대기_배치를_바꾸지_않고_다음_배치에_적용한다()
            throws Exception {

        var state = new AgentModeState(ParsingMode.CENTRAL);

        start(
                smallRaws(150),
                state,
                1,
                stoppingFactory(150)
        );

        // 첫 배치가 큐를 채우고 두 번째 배치가 append에서 기다린다.
        await(this::isWaitingInAppend);

        var first = takeBatch();
        byte[] originalBody = first.body();

        state.requestMode(ParsingMode.AGENT);

        assertArrayEquals(originalBody, queue.take().body());
        assertEquals(
                ParsingMode.CENTRAL,
                state.snapshot().appliedMode()
        );
        assertTrue(state.snapshot().hasPendingRequest());

        assertEquals(
                50,
                inspect(first, ParsingMode.CENTRAL)
                        .path("events").size()
        );

        queue.acknowledge(first);

        await(() -> queue.pendingBatchCount() == 1);

        // 용량을 기다리던 두 번째 배치도 기존 모드여야 한다.
        var second = takeBatch();

        assertEquals(
                50,
                inspect(second, ParsingMode.CENTRAL)
                        .path("events").size()
        );

        queue.acknowledge(second);

        awaitSource();

        // 세 번째 새 배치에서 요청한 모드가 적용된다.
        var third = takeBatch();

        assertEquals(
                50,
                inspect(third, ParsingMode.AGENT)
                        .path("events").size()
        );

        assertEquals(ParsingMode.AGENT, state.snapshot().appliedMode());
        assertFalse(state.snapshot().hasPendingRequest());
        assertEquals(Files.size(inputFile), queue.checkpoint().offset());
    }

    @Test
    void 반대_모드로_재시작해도_기존_배치의_본문을_보존하고_새_입력만_수집한다()
            throws Exception {

        String oldRaw = "{\"message\":\"재시작 전\"}";
        String newRaw = "{\"message\":\"재시작 후\"}";

        start(
                List.of(oldRaw),
                new AgentModeState(ParsingMode.CENTRAL),
                10,
                stoppingFactory(1)
        );

        awaitSource();

        var original = takeBatch();
        byte[] originalBody = original.body();
        long originalOffset = queue.checkpoint().offset();

        // 확인 처리하지 않은 배치를 남긴 상태로 큐를 다시 연다.
        queue.close();
        queue = null;

        Files.writeString(
                inputFile,
                newRaw + "\n",
                StandardCharsets.UTF_8,
                StandardOpenOption.APPEND
        );

        queue = openQueue(10);

        assertEquals(originalOffset, queue.checkpoint().offset());
        assertEquals(1, queue.pendingBatchCount());

        var restartedState = new AgentModeState(ParsingMode.AGENT);
        var restartedFactory = stoppingFactory(1);

        startReader(restartedState, restartedFactory);
        awaitSource();

        assertEquals(2, queue.pendingBatchCount());

        var recovered = takeBatch();

        assertArrayEquals(originalBody, recovered.body());

        var oldDocument = inspect(
                recovered,
                ParsingMode.CENTRAL
        );

        assertEquals(
                oldRaw,
                oldDocument.path("events").get(0).path("raw").asText()
        );

        queue.acknowledge(recovered);

        var added = inspect(takeBatch(), ParsingMode.AGENT);

        assertEquals(1, added.path("events").size());
        assertEquals(
                newRaw,
                added.path("events").get(0).path("raw").asText()
        );
        assertNotEquals(
                oldDocument.path("events").get(0).path("event_id").asText(),
                added.path("events").get(0).path("event_id").asText()
        );

        verify(restartedFactory, times(1))
                .create(anyString(), any(ParsingMode.class));

        assertEquals(Files.size(inputFile), queue.checkpoint().offset());
        assertEquals(
                ParsingMode.AGENT,
                restartedState.snapshot().appliedMode()
        );
    }

    @Test
    void 회전_후_같은_종료_오프셋의_배치를_순서대로_복구한다() throws Exception {
        inputFile = tempDir.resolve("app.log");
        String first = "{\"id\":\"old\"}";
        String next = "{\"id\":\"new\"}";
        Files.writeString(inputFile, first + "\n");
        queue = openQueue(10);
        startRotationReader();
        await(() -> queue.pendingBatchCount() == 1);
        var oldBatch = takeBatch();
        var oldPosition = queue.checkpoint();

        Files.writeString(inputFile, "미완성", StandardOpenOption.APPEND);
        Files.move(inputFile, inputFile.resolveSibling("app.log.1"));
        Files.writeString(inputFile, next + "\n", StandardOpenOption.CREATE_NEW);
        await(() -> queue.pendingBatchCount() == 2);
        stopRotationReader();
        queue.close();

        queue = openQueue(10);
        assertNotEquals(oldPosition.fileId(), queue.checkpoint().fileId());
        assertArrayEquals(oldBatch.body(), takeBatch().body());
        queue.acknowledge(oldBatch);
        var newBatch = takeBatch();
        assertEquals(oldBatch.endOffset(), newBatch.endOffset());
        assertEquals(oldBatch.key().order() + 1, newBatch.key().order());
        assertEquals(next, inspect(newBatch, ParsingMode.CENTRAL)
                .path("events").get(0).path("raw").asText());
    }

    @Test
    void 배치만_확정된_회전을_재시작하면_회전본에서_남은_줄을_읽는다() throws Exception {
        inputFile = tempDir.resolve("app.log");
        Files.writeString(inputFile, "{\"id\":\"first\"}\n");
        queue = openQueue(10);
        Path checkpointFile = tempDir.resolve("queue/checkpoint.properties");
        byte[] beforeBatch = Files.readAllBytes(checkpointFile);

        startRotationReader();
        await(() -> queue.pendingBatchCount() == 1);
        var pending = takeBatch();
        stopRotationReader();
        queue.close();

        // 배치 파일 확정 뒤 체크포인트 확정 전에 종료된 상태를 재현한다.
        Files.write(checkpointFile, beforeBatch);
        String last = "{\"id\":\"last\"}";
        String next = "{\"id\":\"next\"}";
        Files.writeString(inputFile, last + "\n", StandardOpenOption.APPEND);
        Files.move(inputFile, inputFile.resolveSibling("app.log.1"));

        queue = openQueue(10);
        assertEquals(pending.endOffset(), queue.checkpoint().offset());
        assertArrayEquals(pending.body(), takeBatch().body());
        startRotationReader();
        await(() -> queue.pendingBatchCount() == 2);
        Files.writeString(inputFile, next + "\n", StandardOpenOption.CREATE_NEW);
        await(() -> queue.pendingBatchCount() == 3);
        stopRotationReader();

        queue.acknowledge(pending);
        var lastBatch = takeBatch();
        assertEquals(last, inspect(lastBatch, ParsingMode.CENTRAL)
                .path("events").get(0).path("raw").asText());
        queue.acknowledge(lastBatch);
        assertEquals(next, inspect(takeBatch(), ParsingMode.CENTRAL)
                .path("events").get(0).path("raw").asText());
    }

    @Test
    void 기존_큐는_배치_파일을_그대로_승계하고_먼저_전송한다() throws Exception {
        inputFile = tempDir.resolve("app.log");
        byte[] line = "{}\n".getBytes(StandardCharsets.UTF_8);
        byte[] oldBody = "{\"events\":[{\"event_id\":\"legacy\"}]}".getBytes(StandardCharsets.UTF_8);
        Files.write(inputFile, line);
        var attrs = Files.readAttributes(inputFile, java.nio.file.attribute.BasicFileAttributes.class);
        String hash = HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(line));
        Path directory = Files.createDirectory(tempDir.resolve("queue"));
        var properties = new Properties();
        properties.setProperty("input.path", inputFile.toRealPath().toString());
        properties.setProperty("input.key", String.valueOf(attrs.fileKey()));
        properties.setProperty("input.created", attrs.creationTime().toString());
        properties.setProperty("run.id", RUN_ID);
        properties.setProperty("source.id", SOURCE_ID);
        properties.setProperty("endpoint", ENDPOINT.toString());
        properties.setProperty("input.offset", "0");
        try (var output = Files.newOutputStream(directory.resolve("checkpoint.properties"))) {
            properties.store(output, null);
        }

        var content = new ByteArrayOutputStream();
        try (var output = new DataOutputStream(content)) {
            output.writeInt(0x53455331);
            output.writeLong(line.length);
            output.writeUTF(hash);
            output.writeInt(oldBody.length);
            output.write(oldBody);
        }
        content.writeBytes(MessageDigest.getInstance("SHA-256").digest(content.toByteArray()));
        byte[] oldPacket = content.toByteArray();
        Path oldPath = Files.write(directory.resolve("00000000000000000003.batch"), oldPacket);

        queue = openQueue(10);
        assertEquals(line.length, queue.checkpoint().offset());
        assertArrayEquals(oldPacket, Files.readAllBytes(oldPath));
        var legacy = takeBatch();
        assertTrue(legacy.key().legacy());
        assertArrayEquals(oldBody, legacy.body());

        Files.move(inputFile, inputFile.resolveSibling("app.log.1"));
        Files.write(inputFile, line, StandardOpenOption.CREATE_NEW);
        startRotationReader();
        await(() -> queue.pendingBatchCount() == 2);
        assertTrue(takeBatch().key().legacy());
        queue.acknowledge(legacy);
        assertFalse(takeBatch().key().legacy());
        assertEquals(line.length, takeBatch().endOffset());
    }

    @Test
    void 필요한_회전본이_없으면_체크포인트와_미전송_배치를_보존한다() throws Exception {
        inputFile = tempDir.resolve("app.log");
        Files.writeString(inputFile, "{\"id\":\"old\"}\n");
        queue = openQueue(10);
        startRotationReader();
        await(() -> queue.pendingBatchCount() == 1);
        stopRotationReader();
        queue.close();

        Path checkpoint = tempDir.resolve("queue/checkpoint.properties");
        Path batch = tempDir.resolve("queue/seq-00000000000000000001.batch");
        byte[] before = Files.readAllBytes(checkpoint);
        byte[] packet = Files.readAllBytes(batch);
        Files.move(inputFile, inputFile.resolveSibling("unavailable.log"));
        Files.writeString(inputFile, "{\"id\":\"new\"}\n", StandardOpenOption.CREATE_NEW);

        var error = assertThrows(IOException.class, () -> openQueue(10));
        assertTrue(error.getMessage().contains("입력 누락 가능성"));
        assertArrayEquals(before, Files.readAllBytes(checkpoint));
        assertArrayEquals(packet, Files.readAllBytes(batch));
    }

    private void startRotationReader() throws Exception {
        source = new DurableAgentFileSource(
                queue, new AgentEventFactory(new JavaLogProcessor()),
                new AgentModeState(ParsingMode.CENTRAL)
        );
        reader = Thread.ofPlatform().daemon(true).start(source);
    }

    private void stopRotationReader() throws Exception {
        source.close();
        awaitSource();
    }

    private void verifyBatchTransition(
            ParsingMode initialMode,
            ParsingMode nextMode
    ) throws Exception {

        var state = new AgentModeState(initialMode);
        var raws = smallRaws(100);

        var factory = scriptedFactory((number, event) -> {
            if (number == 2) {
                // 첫 이벤트가 수용된 기존 배치의 구성 중에 요청한다.
                state.requestMode(nextMode);

                assertEquals(
                        initialMode,
                        state.snapshot().appliedMode()
                );
                assertTrue(state.snapshot().hasPendingRequest());
            }

            if (number == 100) {
                source.close();
            }
        });

        start(raws, state, 10, factory);
        awaitSource();

        assertEquals(2, queue.pendingBatchCount());

        var first = takeBatch();
        var firstBody = inspect(first, initialMode);

        assertEquals(50, firstBody.path("events").size());

        queue.acknowledge(first);

        var secondBody = inspect(takeBatch(), nextMode);

        assertEquals(50, secondBody.path("events").size());

        var actualRaws = new ArrayList<String>();
        var eventIds = new HashSet<String>();

        for (var body : List.of(firstBody, secondBody)) {
            for (var event : body.path("events")) {
                actualRaws.add(event.path("raw").asText());

                assertTrue(
                        eventIds.add(event.path("event_id").asText()),
                        "새 수집 이벤트의 ID가 중복되었습니다."
                );
            }
        }

        assertEquals(raws, actualRaws);
        assertEquals(100, eventIds.size());
        assertEquals(nextMode, state.snapshot().appliedMode());
        assertFalse(state.snapshot().hasPendingRequest());
    }

    private void verifySizeSplit(
            ParsingMode initialMode,
            ParsingMode nextMode
    ) throws Exception {

        var state = new AgentModeState(initialMode);

        // 각 이벤트는 단독으로 들어가지만 기존 모드에서 합치면 초과한다.
        String firstRaw = largeRaw(
                initialMode == ParsingMode.CENTRAL
                        ? 900_000
                        : 400_000
        );

        String secondRaw = largeRaw(200_000);
        var preparedId = new AtomicReference<UUID>();

        var factory = scriptedFactory((number, event) -> {
            if (number == 2) {
                preparedId.set(event.eventId());
                state.requestMode(nextMode);
                source.close();
            }
        });

        start(
                List.of(firstRaw, secondRaw),
                state,
                10,
                factory
        );

        awaitSource();

        assertEquals(2, queue.pendingBatchCount());

        var first = takeBatch();
        var firstBody = inspect(first, initialMode);

        assertEquals(1, firstBody.path("events").size());
        assertEquals(
                firstRaw,
                firstBody.path("events").get(0).path("raw").asText()
        );

        queue.acknowledge(first);

        var secondBody = inspect(takeBatch(), nextMode);

        assertEquals(1, secondBody.path("events").size());
        assertEquals(
                secondRaw,
                secondBody.path("events").get(0).path("raw").asText()
        );
        assertEquals(
                preparedId.get().toString(),
                secondBody.path("events").get(0).path("event_id").asText()
        );

        // 크기 분할 후 같은 ID로 새 모드의 내용을 준비했는지 확인한다.
        verify(factory).create(
                preparedId.get(),
                secondRaw,
                nextMode
        );

        assertEquals(nextMode, state.snapshot().appliedMode());
        assertFalse(state.snapshot().hasPendingRequest());
    }

    private AgentEventFactory scriptedFactory(
            BiConsumer<Integer, LogEventRequest> afterPrepare
    ) {
        var factory = spy(
                new AgentEventFactory(new JavaLogProcessor())
        );

        var number = new AtomicInteger();

        doAnswer(invocation -> {
            var event = (LogEventRequest) invocation.callRealMethod();

            afterPrepare.accept(
                    number.incrementAndGet(),
                    event
            );

            return event;
        }).when(factory).create(
                anyString(),
                any(ParsingMode.class)
        );

        return factory;
    }

    private AgentEventFactory stoppingFactory(int eventCount) {
        return scriptedFactory((number, event) -> {
            if (number == eventCount) {
                source.close();
            }
        });
    }

    private void start(
            List<String> raws,
            AgentModeState state,
            int maxPendingBatches,
            AgentEventFactory factory
    ) throws Exception {

        inputFile = tempDir.resolve("input.ndjson");

        Files.writeString(
                inputFile,
                String.join("\n", raws) + "\n",
                StandardCharsets.UTF_8
        );

        queue = openQueue(maxPendingBatches);
        startReader(state, factory);
    }

    private AgentFileQueue openQueue(int maxPendingBatches)
            throws Exception {

        return new AgentFileQueue(
                tempDir.resolve("queue"),
                inputFile,
                RUN_ID,
                SOURCE_ID,
                ENDPOINT,
                maxPendingBatches,
                MAX_QUEUE_BYTES
        );
    }

    private void startReader(
            AgentModeState state,
            AgentEventFactory factory
    ) throws Exception {

        source = new DurableAgentFileSource(
                queue,
                factory,
                state,
                () -> 0L,
                MAX_REQUEST_BYTES
        );

        reader = Thread.ofPlatform()
                .name("durable-source-test")
                .daemon(true)
                .unstarted(source);

        reader.start();
    }

    private void awaitSource() throws Exception {
        source.completion()
                .toCompletableFuture()
                .get(15, TimeUnit.SECONDS);

        assertTrue(
                reader.join(Duration.ofSeconds(2)),
                "완료 통지 후 수집 스레드가 종료되지 않았습니다."
        );
    }

    private AgentFileQueue.Batch takeBatch() throws Exception {
        assertTrue(
                queue.pendingBatchCount() > 0,
                "검증할 저장 배치가 없습니다."
        );

        return queue.take();
    }

    private JsonNode inspect(
            AgentFileQueue.Batch batch,
            ParsingMode expectedMode
    ) {
        assertTrue(batch.body().length <= MAX_REQUEST_BYTES);

        var body = mapper.readTree(batch.body());

        assertEquals(RUN_ID, body.path("run_id").asText());
        assertEquals(SOURCE_ID, body.path("source_id").asText());
        assertEquals(expectedMode.name(), body.path("mode").asText());

        var events = body.path("events");

        assertTrue(events.isArray());
        assertTrue(events.size() >= 1 && events.size() <= 50);

        for (var event : events) {
            assertTrue(event.has("raw"));
            UUID.fromString(event.path("event_id").asText());

            if (expectedMode == ParsingMode.CENTRAL) {
                assertFalse(event.has("parse_status"));
                assertFalse(event.has("parsed"));
                assertFalse(event.has("parse_error"));
                continue;
            }

            String raw = event.path("raw").asText();

            if (BROKEN.equals(raw)) {
                assertEquals(
                        "FAILED",
                        event.path("parse_status").asText()
                );
                assertEquals(
                        "INVALID_JSON",
                        event.path("parse_error").path("code").asText()
                );
                assertFalse(event.has("parsed"));
            } else {
                assertEquals(
                        "SUCCESS",
                        event.path("parse_status").asText()
                );
                assertEquals(mapper.readTree(raw), event.get("parsed"));
                assertFalse(event.has("parse_error"));
            }
        }

        return body;
    }

    private boolean isWaitingInAppend() {
        if (queue.pendingBatchCount() != 1
                || reader.getState() != Thread.State.WAITING) {
            return false;
        }

        for (var frame : reader.getStackTrace()) {
            if (frame.getClassName().equals(
                    AgentFileQueue.class.getName()
            ) && frame.getMethodName().equals("append")) {
                return true;
            }
        }

        return false;
    }

    private void await(BooleanSupplier condition) throws Exception {
        long deadline = System.nanoTime()
                + Duration.ofSeconds(15).toNanos();

        while (!condition.getAsBoolean()) {
            if (System.nanoTime() - deadline >= 0) {
                fail("기대한 수집·큐 상태에 도달하지 못했습니다.");
            }

            Thread.sleep(5);
        }
    }

    private List<String> smallRaws(int count) {
        var raws = new ArrayList<String>();

        for (int index = 0; index < count; index++) {
            raws.add(
                    index % 10 == 0
                            ? BROKEN
                            : "{\"index\":" + index + "}"
            );
        }

        return raws;
    }

    private String largeRaw(int length) {
        return "{\"message\":\"" + "x".repeat(length) + "\"}";
    }
}
