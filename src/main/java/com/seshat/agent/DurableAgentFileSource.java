package com.seshat.agent;

import com.seshat.ingestion.dto.ParsingMode;
import tools.jackson.databind.DeserializationFeature;
import tools.jackson.databind.json.JsonMapper;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.LongSupplier;

import static java.nio.file.StandardOpenOption.READ;

/**
 * 같은 원본 파일을 체크포인트부터 이어 읽는 수집기.
 * <p>
 * - LF 또는 CRLF로 끝난 줄만 수집한다.
 * - 최대 50건 또는 첫 이벤트 대기 200ms로 배치를 형성한다.
 * - 완성된 HTTP 본문과 입력 종료 위치를 파일 큐에 보관한다.
 * - 파일 회전·축소는 오류로 전달한다.
 * <p>
 * 하나의 전용 수집 스레드에서만 실행한다.
 */
public final class DurableAgentFileSource
        implements Runnable, AutoCloseable {

    private static final int MAX_BATCH_SIZE = 50;
    private static final int MAX_REQUEST_BYTES = 1_048_576;
    private static final int MAX_LINE_BYTES = 1_048_576;

    private static final long BATCH_WAIT_NANOS =
            Duration.ofMillis(200).toNanos();

    private final AgentFileQueue queue;
    private final AgentEventFactory eventFactory;
    private final AgentModeState modeState;
    private final LongSupplier nanoTime;

    private final JsonMapper mapper = JsonMapper.builder()
            .enable(DeserializationFeature.USE_BIG_INTEGER_FOR_INTS)
            .enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS)
            .build();

    private final AtomicBoolean started = new AtomicBoolean();
    private final AtomicBoolean stopRequested = new AtomicBoolean();

    private final CompletableFuture<Void> completion =
            new CompletableFuture<>();

    // 직렬화된 이벤트만 최대 50건 보관한다.
    // 미전송 배치의 장기 보관 책임은 파일 큐에 있다.
    private final List<byte[]> events = new ArrayList<>();
    private final byte[] batchSuffix =
            "]}".getBytes(StandardCharsets.UTF_8);
    // 구성 중인 배치가 있을 때만 사용한다.
    private AgentModeState.BatchSelection batchSelection;
    private byte[] batchPrefix;
    private int batchBytes;
    private long batchStartedAt;
    private long batchEndOffset;

    public DurableAgentFileSource(
            AgentFileQueue queue,
            AgentEventFactory eventFactory,
            AgentModeState modeState
    ) throws IOException {
        this(queue, eventFactory, modeState, System::nanoTime);
    }

    /**
     * 배치 시간 경계를 재현할 수 있도록 시간 공급자를 받는다.
     */
    DurableAgentFileSource(
            AgentFileQueue queue,
            AgentEventFactory eventFactory,
            AgentModeState modeState,
            LongSupplier nanoTime
    ) throws IOException {
        this.queue = Objects.requireNonNull(queue);
        this.eventFactory = Objects.requireNonNull(eventFactory);
        this.modeState = Objects.requireNonNull(modeState);
        this.nanoTime = Objects.requireNonNull(nanoTime);

        queue.validateInputFile();

        // 초기 모드의 메타데이터 크기는 시작 전에 검사한다.
        createBatchPrefix(modeState.snapshot().appliedMode());
    }

    @Override
    public void run() {
        if (!started.compareAndSet(false, true)) {
            throw new IllegalStateException(
                    "File source can only run once."
            );
        }

        try (FileChannel input = FileChannel.open(
                queue.inputFile(),
                READ
        )) {
            long offset = queue.checkpoint().offset();
            input.position(offset);

            boolean firstLine = offset == 0;

            ByteBuffer readBuffer = ByteBuffer.allocate(8192);
            ByteArrayOutputStream line =
                    new ByteArrayOutputStream();

            while (!stopRequested.get()) {
                validateInput(input);
                flushIfDue();

                readBuffer.clear();
                int count = input.read(readBuffer);

                if (count < 0) {
                    // 마지막 조각은 줄바꿈이 추가될 때까지 유지한다.
                    Thread.sleep(25);
                    continue;
                }

                if (count == 0) {
                    Thread.sleep(25);
                    continue;
                }

                readBuffer.flip();

                while (readBuffer.hasRemaining()
                        && !stopRequested.get()) {

                    flushIfDue();

                    int value = Byte.toUnsignedInt(
                            readBuffer.get()
                    );

                    offset++;

                    if (value != '\n') {
                        if (line.size() >= MAX_LINE_BYTES) {
                            flushBatch();

                            throw new IllegalArgumentException(
                                    "Input line exceeds the 1MiB limit."
                            );
                        }

                        line.write(value);
                        continue;
                    }

                    String raw = decodeLine(
                            line.toByteArray()
                    );

                    if (firstLine && raw.startsWith("\uFEFF")) {
                        throw new IllegalArgumentException(
                                "UTF-8 BOM is unsupported."
                        );
                    }

                    appendEvent(raw, offset);

                    firstLine = false;
                    line.reset();
                }
            }

            // 종료 요청 시 이미 완성된 줄은 파일 큐에 보관한다.
            // 줄바꿈 없는 마지막 조각의 위치는 확정하지 않는다.
            flushBatch();

            completion.complete(null);

        } catch (Throwable error) {
            if (error instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }

            completion.completeExceptionally(error);

            if (error instanceof Error fatal) {
                throw fatal;
            }
        }
    }

    private byte[] createBatchPrefix(ParsingMode mode) {
        String prefix =
                "{\"run_id\":"
                        + mapper.writeValueAsString(queue.runId())
                        + ",\"source_id\":"
                        + mapper.writeValueAsString(queue.sourceId())
                        + ",\"mode\":"
                        + mapper.writeValueAsString(mode.name())
                        + ",\"events\":[";

        byte[] bytes = prefix.getBytes(StandardCharsets.UTF_8);

        if (emptyBatchBytes(bytes) >= MAX_REQUEST_BYTES) {
            throw new IllegalArgumentException(
                    "배치 메타데이터가 HTTP 요청 크기 제한을 초과합니다."
            );
        }

        return bytes;
    }

    private void validateInput(FileChannel input)
            throws IOException {

        queue.validateInputFile();

        // 마지막 체크포인트 이후까지 읽은 상태에서
        // 파일이 축소된 경우도 감지한다.
        if (Files.size(queue.inputFile()) < input.position()) {
            throw new IOException(
                    "Input truncation detected while reading."
            );
        }
    }

    private String decodeLine(byte[] bytes)
            throws IOException {

        int length = bytes.length;

        // CRLF의 CR만 제거한다.
        if (length > 0 && bytes[length - 1] == '\r') {
            length--;
        }

        return StandardCharsets.UTF_8.newDecoder()
                .onMalformedInput(CodingErrorAction.REPORT)
                .onUnmappableCharacter(CodingErrorAction.REPORT)
                .decode(ByteBuffer.wrap(bytes, 0, length))
                .toString();
    }

    private void appendEvent(
            String raw,
            long endOffset
    ) throws IOException, InterruptedException {

        long admittedAt = nanoTime.getAsLong();

        AgentModeState.BatchSelection selection;
        byte[] prefix;

        if (events.isEmpty()) {
            selection = modeState.selectForNextBatch();
            prefix = createBatchPrefix(selection.mode());
        } else {
            selection = batchSelection;
            prefix = batchPrefix;
        }

        var event = eventFactory.create(raw, selection.mode());
        byte[] serialized = mapper.writeValueAsBytes(event);

        // 현재 배치에 들어가지 않으면 기존 배치를 먼저 영속 보관한다.
        if (!events.isEmpty()
                && (long) batchBytes + 1 + serialized.length
                > MAX_REQUEST_BYTES) {

            ParsingMode preparedMode = selection.mode();

            flushBatch();

            // 큐 용량 대기가 끝난 뒤 다음 배치의 요청 상태를 예약한다.
            selection = modeState.selectForNextBatch();
            prefix = createBatchPrefix(selection.mode());
            admittedAt = nanoTime.getAsLong();

            if (selection.mode() != preparedMode) {
                // 아직 배치에 수용하지 않은 이벤트만 같은 ID로 다시 준비한다.
                event = eventFactory.create(
                        event.eventId(),
                        raw,
                        selection.mode()
                );

                serialized = mapper.writeValueAsBytes(event);
            }
        }

        // 모드가 바뀌면 파싱 결과와 헤더 크기도 달라질 수 있다.
        if ((long) emptyBatchBytes(prefix) + serialized.length
                > MAX_REQUEST_BYTES) {

            throw new IllegalArgumentException(
                    "단일 이벤트가 HTTP 요청 크기 제한을 초과합니다. "
                            + "입력 종료 위치: " + endOffset
            );
        }

        boolean firstEvent = events.isEmpty();
        int separatorBytes = firstEvent ? 0 : 1;

        if (firstEvent) {
            batchSelection = selection;
            batchPrefix = prefix;
            batchBytes = emptyBatchBytes(prefix);
            batchStartedAt = admittedAt;
        }

        events.add(serialized);
        batchBytes += separatorBytes + serialized.length;
        batchEndOffset = endOffset;

        if (firstEvent) {
            // 첫 이벤트를 실제 수용한 배치의 모드만 적용 상태로 보고한다.
            modeState.markApplied(selection);
        }

        if (events.size() >= MAX_BATCH_SIZE) {
            flushBatch();
        } else {
            flushIfDue();
        }
    }

    private void flushIfDue()
            throws IOException, InterruptedException {

        if (!events.isEmpty()
                && nanoTime.getAsLong() - batchStartedAt
                >= BATCH_WAIT_NANOS) {

            flushBatch();
        }
    }

    private void flushBatch()
            throws IOException, InterruptedException {

        if (events.isEmpty()) {
            return;
        }

        var body = new ByteArrayOutputStream(batchBytes);
        body.writeBytes(batchPrefix);

        for (int index = 0; index < events.size(); index++) {
            if (index > 0) {
                body.write(',');
            }

            body.writeBytes(events.get(index));
        }

        body.writeBytes(batchSuffix);

        // 큐 보관이 성공하기 전에는 배치와 예약 모드를 유지한다.
        queue.append(
                batchEndOffset,
                body.toByteArray()
        );

        events.clear();
        batchSelection = null;
        batchPrefix = null;
        batchBytes = 0;
        batchStartedAt = 0;
        batchEndOffset = 0;
    }

    private int emptyBatchBytes(byte[] prefix) {
        return prefix.length + batchSuffix.length;
    }

    public CompletionStage<Void> completion() {
        return completion.minimalCompletionStage();
    }

    /**
     * 정상 종료 요청.
     * 진행 중인 파일 큐 보관을 강제로 중단하지 않는다.
     */
    @Override
    public void close() {
        stopRequested.set(true);
    }
}