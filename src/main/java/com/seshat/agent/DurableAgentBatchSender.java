package com.seshat.agent;

import com.seshat.ingestion.dto.LogBatchResponse;
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
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * 파일 큐의 가장 오래된 배치를 순서대로 전송한다.
 * <p>
 * - 중앙 확인 전에는 배치 파일을 삭제하지 않는다.
 * - 재시도에서 같은 HTTP 본문을 사용한다.
 * - 202와 응답 건수를 검증한 후 큐를 확인 처리한다.
 * <p>
 * 하나의 전용 전송 스레드에서만 실행한다.
 */
public final class DurableAgentBatchSender
        implements Runnable, AutoCloseable {

    private static final long INITIAL_RETRY_DELAY_MS = 1_000;
    private static final long MAX_RETRY_DELAY_MS = 60_000;

    private static final System.Logger log =
            System.getLogger(
                    DurableAgentBatchSender.class.getName()
            );

    private final AgentFileQueue queue;
    private final URI endpoint;

    private final JsonMapper mapper = JsonMapper.builder()
            .enable(DeserializationFeature.USE_BIG_INTEGER_FOR_INTS)
            .enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS)
            .build();

    private final AtomicBoolean started = new AtomicBoolean();
    private final AtomicBoolean stopRequested = new AtomicBoolean();

    private final AtomicReference<Thread> worker =
            new AtomicReference<>();

    private final CompletableFuture<Void> completion =
            new CompletableFuture<>();

    public DurableAgentBatchSender(AgentFileQueue queue) {
        this.queue = Objects.requireNonNull(queue);
        this.endpoint = queue.endpoint();

        String scheme = endpoint.getScheme();

        if (!endpoint.isAbsolute()
                || endpoint.getHost() == null
                || (!"http".equalsIgnoreCase(scheme)
                && !"https".equalsIgnoreCase(scheme))) {

            throw new IllegalArgumentException(
                    "Endpoint must be an absolute HTTP or HTTPS URL."
            );
        }
    }

    @Override
    public void run() {
        if (!started.compareAndSet(false, true)) {
            throw new IllegalStateException(
                    "Batch sender can only run once."
            );
        }

        worker.set(Thread.currentThread());

        Throwable failure = null;

        try (HttpClient client = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(3))
                .followRedirects(HttpClient.Redirect.NEVER)
                .build()) {

            while (!stopRequested.get()) {
                // 배치를 꺼내도 파일은 삭제되지 않는다.
                AgentFileQueue.Batch batch = queue.take();

                // 이후 재시도에서는 이 배열을 변경하지 않는다.
                byte[] body = batch.body();

                int expectedCount = expectedEventCount(body);

                sendUntilAcknowledged(
                        client,
                        body,
                        expectedCount
                );

                if (stopRequested.get()) {
                    // 중앙 확인을 받았더라도 종료가 요청되면
                    // 파일을 남긴다. 다음 실행에서 같은 본문으로 다시 확인받을 수 있다.
                    break;
                }

                // 중앙 확인을 받은 뒤에만 파일을 삭제한다.
                queue.acknowledge(batch);
            }

        } catch (InterruptedException error) {
            Thread.currentThread().interrupt();

            if (!stopRequested.get()) {
                failure = error;
            }

        } catch (Throwable error) {
            failure = error;

        } finally {
            worker.set(null);

            if (failure == null) {
                completion.complete(null);
            } else {
                completion.completeExceptionally(failure);
            }
        }

        if (failure instanceof Error fatal) {
            throw fatal;
        }
    }

    /**
     * 저장된 HTTP 본문의 이벤트 수를 확인한다.
     * raw 필드 안의 JSON을 로그 파서로 파싱하는 처리는 아니다.
     */
    private int expectedEventCount(byte[] body) {
        var request = mapper.readTree(body);

        if (request == null || !request.isObject()) {
            throw new IllegalStateException(
                    "Stored HTTP body must be a JSON object."
            );
        }

        if (!queue.runId().equals(
                request.path("run_id").asText()
        ) || !queue.sourceId().equals(
                request.path("source_id").asText()
        )) {
            throw new IllegalStateException(
                    "Stored batch identity differs from the queue."
            );
        }

        String mode = request.path("mode").asText();

        if (!"CENTRAL".equals(mode) && !"AGENT".equals(mode)) {
            throw new IllegalStateException(
                    "Stored batch has an invalid parsing mode."
            );
        }

        var events = request.path("events");

        if (!events.isArray()
                || events.size() < 1
                || events.size() > 50) {

            throw new IllegalStateException(
                    "Stored batch must contain 1 to 50 events."
            );
        }

        return events.size();
    }

    private void sendUntilAcknowledged(
            HttpClient client,
            byte[] body,
            int expectedCount
    ) throws InterruptedException {

        // 재시도마다 다시 직렬화하지 않고 같은 요청 본문을 사용한다.
        HttpRequest request = HttpRequest.newBuilder(endpoint)
                .timeout(Duration.ofSeconds(10))
                .header("Content-Type", "application/json")
                .header("Accept", "application/json")
                .POST(HttpRequest.BodyPublishers.ofByteArray(body))
                .build();

        long retryDelay = INITIAL_RETRY_DELAY_MS;

        while (true) {
            checkStopping();

            HttpResponse<String> response;

            try {
                response = client.send(
                        request,
                        HttpResponse.BodyHandlers.ofString(
                                StandardCharsets.UTF_8
                        )
                );

            } catch (IOException error) {
                log.log(
                        System.Logger.Level.WARNING,
                        "Batch transport failed; stored batch retained.",
                        error
                );

                pauseBeforeRetry(retryDelay);
                retryDelay = nextRetryDelay(retryDelay);
                continue;
            }

            int status = response.statusCode();

            if (status == 202) {
                if (isValidAcknowledgement(
                        response.body(),
                        expectedCount
                )) {
                    return;
                }

                // 중앙 커밋은 성공했지만 응답만 깨졌을 수 있다.
                log.log(
                        System.Logger.Level.WARNING,
                        "Invalid acknowledgement; retrying the same batch."
                );

            } else if (isRetryable(status)) {
                log.log(
                        System.Logger.Level.WARNING,
                        "Temporary batch failure: HTTP " + status
                );

            } else {
                // 요청·계약 오류 등은 무한 재시도하지 않는다.
                // 예외를 전달하고 파일은 남겨 둔다.
                throw new IllegalStateException(
                        "Batch rejected with HTTP " + status
                                + "; stored batch retained."
                );
            }

            pauseBeforeRetry(retryDelay);
            retryDelay = nextRetryDelay(retryDelay);
        }
    }

    private boolean isValidAcknowledgement(
            String body,
            int expectedCount
    ) {
        try {
            LogBatchResponse response = mapper.readValue(
                    body,
                    LogBatchResponse.class
            );

            return response != null
                    && response.acceptedCount() == expectedCount
                    && response.insertedCount() >= 0
                    && response.duplicateCount() >= 0
                    && (long) response.insertedCount()
                    + response.duplicateCount()
                    == expectedCount;

        } catch (JacksonException error) {
            return false;
        }
    }

    private boolean isRetryable(int status) {
        return status == 408
                || status == 429
                || (status >= 500 && status <= 599);
    }

    private long nextRetryDelay(long current) {
        return Math.min(MAX_RETRY_DELAY_MS, current * 2);
    }

    private void pauseBeforeRetry(long delay)
            throws InterruptedException {

        checkStopping();
        Thread.sleep(delay);
    }

    private void checkStopping() throws InterruptedException {
        if (stopRequested.get()
                || Thread.currentThread().isInterrupted()) {

            throw new InterruptedException(
                    "Batch sender stop requested."
            );
        }
    }

    public CompletionStage<Void> completion() {
        return completion.minimalCompletionStage();
    }

    /**
     * 전송 스레드의 중단만 요청한다.
     * 파일 큐의 종료는 실행 관리자에서 담당한다.
     */
    @Override
    public void close() {
        stopRequested.set(true);

        Thread thread = worker.get();

        if (thread != null) {
            thread.interrupt();

        } else if (!started.get()) {
            completion.complete(null);
        }
    }
}