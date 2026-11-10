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
    private final ParsingMode mode;

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

    private final byte[] batchPrefix;
    private final byte[] batchSuffix =
            "]}".getBytes(StandardCharsets.UTF_8);

    private int batchBytes;
    private long batchStartedAt;
    private long batchEndOffset;

    public DurableAgentFileSource(
            AgentFileQueue queue,
            AgentEventFactory eventFactory,
            ParsingMode mode
    ) throws IOException {

        this.queue = Objects.requireNonNull(queue);
        this.eventFactory = Objects.requireNonNull(eventFactory);
        this.mode = Objects.requireNonNull(mode);

        queue.validateInputFile();

        // 문자열 값은 JSON 직렬화를 거쳐 이스케이프한다.
        String prefix =
                "{\"run_id\":"
                        + mapper.writeValueAsString(queue.runId())
                        + ",\"source_id\":"
                        + mapper.writeValueAsString(queue.sourceId())
                        + ",\"mode\":"
                        + mapper.writeValueAsString(mode.name())
                        + ",\"events\":[";

        batchPrefix = prefix.getBytes(StandardCharsets.UTF_8);
        batchBytes = emptyBatchBytes();

        if (batchBytes >= MAX_REQUEST_BYTES) {
            throw new IllegalArgumentException(
                    "Batch metadata exceeds the request limit."
            );
        }
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

        long admittedAt = System.nanoTime();

        var event = eventFactory.create(raw, mode);
        byte[] serialized = mapper.writeValueAsBytes(event);

        // 단일 이벤트조차 전송할 수 없으면 앞선 배치를 보관하고 중단한다.
        if ((long) emptyBatchBytes() + serialized.length
                > MAX_REQUEST_BYTES) {

            flushBatch();

            throw new IllegalArgumentException(
                    "A single event exceeds the HTTP request limit. "
                            + "Input end offset: " + endOffset
            );
        }

        int separatorBytes = events.isEmpty() ? 0 : 1;

        // 현재 배치에 넣으면 초과하는 경우 앞선 이벤트부터 보관한다.
        if ((long) batchBytes
                + separatorBytes
                + serialized.length
                > MAX_REQUEST_BYTES) {

            flushBatch();
            separatorBytes = 0;
        }

        if (events.isEmpty()) {
            batchStartedAt = admittedAt;
        }

        events.add(serialized);

        batchBytes += separatorBytes + serialized.length;
        batchEndOffset = endOffset;

        if (events.size() >= MAX_BATCH_SIZE) {
            flushBatch();
        } else {
            flushIfDue();
        }
    }

    private void flushIfDue()
            throws IOException, InterruptedException {

        if (!events.isEmpty()
                && System.nanoTime() - batchStartedAt
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

        // 파일 큐 보관이 성공한 뒤에만 현재 배치를 비운다.
        // 큐가 가득 차면 여기서 대기하여 수집에 역압을 적용한다.
        queue.append(
                batchEndOffset,
                body.toByteArray()
        );

        events.clear();
        batchBytes = emptyBatchBytes();
        batchStartedAt = 0;
    }

    private int emptyBatchBytes() {
        return batchPrefix.length + batchSuffix.length;
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