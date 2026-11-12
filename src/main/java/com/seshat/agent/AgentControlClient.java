package com.seshat.agent;

import com.seshat.control.dto.AgentControlMessages.SyncRequest;
import com.seshat.control.dto.AgentControlMessages.SyncResponse;
import com.seshat.control.dto.AgentControlMessages.Target;
import tools.jackson.databind.json.JsonMapper;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

final class AgentControlClient implements AutoCloseable {

    private static final Duration SYNC_INTERVAL = Duration.ofSeconds(2);
    private static final Duration REQUEST_TIMEOUT = Duration.ofSeconds(2);
    private static final System.Logger log =
            System.getLogger(AgentControlClient.class.getName());

    private final URI endpoint;
    private final Target target;
    private final AgentModeState modeState;
    private final JsonMapper mapper = JsonMapper.builder().build();
    private final Thread worker;

    private final Set<UUID> retiredControllers = new HashSet<>();
    private UUID controllerId;
    private volatile boolean stopped;

    AgentControlClient(URI endpoint, Target target, AgentModeState modeState) {
        this.endpoint = Objects.requireNonNull(endpoint);
        this.target = Objects.requireNonNull(target);
        this.modeState = Objects.requireNonNull(modeState);

        worker = Thread.ofPlatform()
                .name("seshat-agent-control")
                .daemon(true)
                .unstarted(this::run);
    }

    void start() {
        worker.start();
    }

    private void run() {
        HttpClient client = null;

        try {
            client = HttpClient.newBuilder()
                    .connectTimeout(REQUEST_TIMEOUT)
                    .followRedirects(HttpClient.Redirect.NEVER)
                    .version(HttpClient.Version.HTTP_1_1)
                    .build();

            boolean failureReported = false;
            while (!stopped) {
                try {
                    sync(client);
                    failureReported = false;
                } catch (IOException | RuntimeException error) {
                    modeState.suspendControl();

                    if (!stopped && !failureReported) {
                        log.log(System.Logger.Level.WARNING,
                                "제어 동기화 실패. 현재 적용 모드로 수집을 계속합니다.", error);
                    }
                    failureReported = true;
                }

                if (!stopped) {
                    Thread.sleep(SYNC_INTERVAL);
                }
            }
        } catch (InterruptedException error) {
            Thread.currentThread().interrupt();
        } catch (RuntimeException error) {
            if (!stopped) {
                log.log(System.Logger.Level.WARNING,
                        "제어 워커를 종료합니다. 로그 수집은 계속됩니다.", error);
            }
        } finally {
            modeState.suspendControl();
            if (client != null) {
                client.shutdownNow();
            }
        }
    }

    private void sync(HttpClient client) throws IOException, InterruptedException {
        var snapshot = modeState.snapshot();
        var report = new SyncRequest(
                target,
                snapshot.receivedCommandId(),
                snapshot.appliedCommandId(),
                snapshot.appliedMode()
        );

        var request = HttpRequest.newBuilder(endpoint)
                .header("Content-Type", "application/json")
                .header("Accept", "application/json")
                .POST(HttpRequest.BodyPublishers.ofByteArray(mapper.writeValueAsBytes(report)))
                .build();

        // 중앙의 응답 처리 시간까지 포함해 유효 기간을 보수적으로 계산한다.
        long startedAt = System.nanoTime();
        var exchange = client.sendAsync(request, HttpResponse.BodyHandlers.ofByteArray());
        HttpResponse<byte[]> response;
        try {
            response = exchange.get(REQUEST_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        } catch (ExecutionException | TimeoutException error) {
            throw new IOException("제어 동기화 응답을 받지 못했습니다.", error);
        } finally {
            if (!exchange.isDone()) {
                exchange.cancel(true);
            }
        }

        if (response.statusCode() != 200) {
            throw new IOException("제어 동기화 응답 HTTP " + response.statusCode());
        }

        var result = mapper.readValue(response.body(), SyncResponse.class);
        applyResponse(result, startedAt);
    }

    private synchronized void applyResponse(SyncResponse response, long startedAt)
            throws IOException {
        if (stopped) {
            return;
        }
        if (response == null || !target.equals(response.target())) {
            throw new IOException("제어 응답의 대상 인스턴스가 요청과 다릅니다.");
        }

        UUID nextController = response.controllerInstanceId();
        if (nextController == null || retiredControllers.contains(nextController)) {
            throw new IOException("제어 응답의 중앙 인스턴스가 유효하지 않습니다.");
        }

        var command = response.command();
        if (command != null && !nextController.equals(command.id().controllerInstanceId())) {
            throw new IOException("명령과 제어 응답의 중앙 인스턴스가 다릅니다.");
        }

        modeState.acceptControl(command, startedAt, response.validForMs());

        if (controllerId != null && !controllerId.equals(nextController)) {
            retiredControllers.add(controllerId);
        }
        controllerId = nextController;
    }

    @Override
    public void close() {
        synchronized (this) {
            if (stopped) {
                return;
            }
            stopped = true;
            modeState.suspendControl();
        }

        worker.interrupt();
        try {
            worker.join(Duration.ofSeconds(3));
        } catch (InterruptedException error) {
            Thread.currentThread().interrupt();
        }
    }
}