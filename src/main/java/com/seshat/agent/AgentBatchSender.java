package com.seshat.agent;

import com.seshat.ingestion.dto.LogBatchRequest;
import com.seshat.ingestion.dto.LogBatchResponse;
import com.seshat.ingestion.dto.ParsingMode;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.DeserializationFeature;
import tools.jackson.databind.json.JsonMapper;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * 하나의 전송 스레드에서만 사용한다.
 * close()는 전송 스레드 종료 후 호출한다.
 */
public final class AgentBatchSender implements AutoCloseable {

    private static final System.Logger log =
            System.getLogger(AgentBatchSender.class.getName());

    private static final int MAX_BATCH_SIZE = 50;

    private static final long BATCH_WAIT_NANOS =
            TimeUnit.MILLISECONDS.toNanos(200);

    private final AgentBuffer buffer;
    private final URI endpoint;
    private final String runId;
    private final String sourceId;
    private final ParsingMode mode;
    private final int maxRequestBytes;

    private final JsonMapper mapper = JsonMapper.builder()
            .enable(DeserializationFeature.USE_BIG_INTEGER_FOR_INTS)
            .enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS)
            .build();

    private final HttpClient client = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(3))
            .followRedirects(HttpClient.Redirect.NEVER)
            .build();

    // 큐에서 꺼냈지만 중앙 확인을 받지 못한 이벤트.
    private final ArrayDeque<AgentBuffer.Entry> pending =
            new ArrayDeque<>();

    // 재전송에서는 이 배치의 본문을 그대로 사용한다.
    private WireBatch currentBatch;

    public AgentBatchSender(
            AgentBuffer buffer,
            URI endpoint,
            String runId,
            String sourceId,
            ParsingMode mode
    ) {
        this(buffer, endpoint, runId, sourceId, mode,
                AgentBatchLimits.MAX_REQUEST_BYTES);
    }

    AgentBatchSender(
            AgentBuffer buffer,
            URI endpoint,
            String runId,
            String sourceId,
            ParsingMode mode,
            int maxRequestBytes
    ) {
        this.buffer = Objects.requireNonNull(buffer);
        this.endpoint = Objects.requireNonNull(endpoint);
        this.runId = requireNonBlank(runId, "runId");
        this.sourceId = requireNonBlank(sourceId, "sourceId");
        this.mode = Objects.requireNonNull(mode);
        this.maxRequestBytes = maxRequestBytes;
    }

    private static String requireNonBlank(
            String value,
            String name
    ) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(
                    name + " must not be blank."
            );
        }

        return value;
    }

    /**
     * 한 HTTP 배치를 중앙에 확인받을 때까지 처리한다.
     * 반환값은 중앙에서 보관을 확인한 이벤트 수다.
     */
    public int runOnce() throws InterruptedException {
        if (currentBatch == null) {
            collectIfNeeded();
            currentBatch = prepareBatch();
        }

        sendUntilAcknowledged(currentBatch);

        int confirmedCount = currentBatch.entries().size();

        for (int index = 0; index < confirmedCount; index++) {
            pending.removeFirst().acknowledge();
        }

        currentBatch = null;
        return confirmedCount;
    }

    private void collectIfNeeded() throws InterruptedException {
        // 크기 분할로 남은 이벤트는 먼저 전송한다.
        if (!pending.isEmpty()) {
            return;
        }

        pending.addLast(buffer.take());

        long firstAdmittedAt =
                pending.getFirst().admittedAtNanos();

        while (pending.size() < MAX_BATCH_SIZE) {
            long elapsed =
                    System.nanoTime() - firstAdmittedAt;

            long remaining =
                    Math.max(0, BATCH_WAIT_NANOS - elapsed);

            // 기한이 지났어도 이미 대기 중인 이벤트는 즉시 가져온다.
            var next = buffer.poll(remaining);

            if (next == null) {
                break;
            }

            // 이후 interrupt가 발생해도 필드에 보관된다.
            pending.addLast(next);
        }
    }

    private WireBatch prepareBatch() {
        var entries = List.copyOf(pending);
        int count = entries.size();

        while (true) {
            var selected =
                    List.copyOf(entries.subList(0, count));

            var request = new LogBatchRequest(
                    runId,
                    sourceId,
                    mode,
                    selected.stream()
                            .map(AgentBuffer.Entry::event)
                            .toList()
            );

            byte[] body = mapper.writeValueAsBytes(request);

            if (body.length <= maxRequestBytes) {
                return new WireBatch(selected, body);
            }

            if (count == 1) {
                throw new IllegalStateException(
                        "A single event exceeds the request limit: "
                                + selected.getFirst()
                                .event()
                                .eventId()
                );
            }

            // 우선 단순한 이분 분할로 구현한다.
            // 선택하지 않은 이벤트는 pending에 남는다.
            count = Math.max(1, count / 2);
        }
    }

    private void sendUntilAcknowledged(WireBatch batch)
            throws InterruptedException {

        var request = HttpRequest.newBuilder(endpoint)
                .timeout(Duration.ofSeconds(10))
                .header("Content-Type", "application/json")
                .header("Accept", "application/json")
                .POST(HttpRequest.BodyPublishers.ofByteArray(
                        batch.body()
                ))
                .build();

        long retryDelayMillis = 1_000;

        while (true) {
            if (Thread.currentThread().isInterrupted()) {
                throw new InterruptedException(
                        "Agent sender was interrupted."
                );
            }

            HttpResponse<String> response;

            try {
                response = client.send(
                        request,
                        HttpResponse.BodyHandlers.ofString(
                                StandardCharsets.UTF_8
                        )
                );
            } catch (IOException exception) {
                log.log(
                        System.Logger.Level.WARNING,
                        "Batch transport failed. Retrying.",
                        exception
                );

                Thread.sleep(retryDelayMillis);
                retryDelayMillis =
                        nextRetryDelay(retryDelayMillis);
                continue;
            }

            int status = response.statusCode();

            if (status == 202) {
                if (isValidAcknowledgement(
                        response.body(),
                        batch.entries().size()
                )) {
                    return;
                }

                // 커밋 후 응답만 깨졌을 수도 있으므로
                // 같은 ID와 본문으로 다시 확인한다.
                log.log(
                        System.Logger.Level.WARNING,
                        "Invalid batch acknowledgement. Retrying."
                );
            } else if (!isRetryable(status)) {
                throw new IllegalStateException(
                        "Batch rejected with HTTP " + status
                                + ". First event ID: "
                                + batch.entries().getFirst()
                                .event()
                                .eventId()
                );
            } else {
                log.log(
                        System.Logger.Level.WARNING,
                        "Temporary batch failure: HTTP " + status
                );
            }

            Thread.sleep(retryDelayMillis);
            retryDelayMillis =
                    nextRetryDelay(retryDelayMillis);
        }
    }

    private boolean isValidAcknowledgement(
            String body,
            int sentCount
    ) {
        try {
            var response =
                    mapper.readValue(body, LogBatchResponse.class);

            return response != null
                    && response.acceptedCount() == sentCount
                    && response.insertedCount() >= 0
                    && response.duplicateCount() >= 0
                    && (long) response.insertedCount()
                    + response.duplicateCount()
                    == sentCount;
        } catch (JacksonException exception) {
            return false;
        }
    }

    private boolean isRetryable(int status) {
        return status == 408
                || status == 429
                || (status >= 500 && status <= 599);
    }

    private long nextRetryDelay(long current) {
        return Math.min(60_000, current * 2);
    }

    @Override
    public void close() {
        client.close();
    }

    private record WireBatch(
            List<AgentBuffer.Entry> entries,
            byte[] body
    ) {
    }
}
