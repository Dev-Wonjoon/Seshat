package com.seshat.agent;

import com.seshat.monitoring.HostMemory;
import com.seshat.monitoring.dto.AgentHeartbeatRequest;
import tools.jackson.databind.json.JsonMapper;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

public final class AgentHeartbeatReporter implements AutoCloseable {

    private static final System.Logger log =
            System.getLogger(AgentHeartbeatReporter.class.getName());

    private final AgentFileQueue queue;
    private final AgentModeState modeState;
    private final int maxPendingBatches;
    private final long maxPendingBytes;

    private final UUID instanceId;

    private final JsonMapper mapper = JsonMapper.builder().build();

    private final AtomicBoolean stopped = new AtomicBoolean();

    private final Thread worker;

    private volatile HttpClient activeClient;

    private boolean cpuPrimed;

    public AgentHeartbeatReporter(
            AgentFileQueue queue,
            AgentModeState modeState,
            int maxPendingBatches,
            long maxPendingBytes
    ) {
        this(queue, modeState, UUID.randomUUID(), maxPendingBatches, maxPendingBytes);
    }

    AgentHeartbeatReporter(
            AgentFileQueue queue,
            AgentModeState modeState,
            UUID instanceId,
            int maxPendingBatches,
            long maxPendingBytes
    ) {
        this.queue = Objects.requireNonNull(queue);
        this.modeState = Objects.requireNonNull(modeState);
        this.instanceId = Objects.requireNonNull(instanceId);
        this.maxPendingBatches = maxPendingBatches;
        this.maxPendingBytes = maxPendingBytes;

        worker = Thread.ofPlatform()
                .name("seshat-agent-heartbeat")
                .daemon(true)
                .unstarted(this::run);
    }

    private static Double validLoad(double value) {
        return Double.isFinite(value) && value >= 0 && value <= 1
                ? value
                : null;
    }

    public void start() {
        worker.start();
    }

    private void run() {
        HttpClient client = null;

        try {
            long intervalMs = Long.getLong(
                    "seshat.agent.heartbeat-interval-ms",
                    5_000L
            );

            if (intervalMs < 100) {
                throw new IllegalArgumentException(
                        "heartbeat-interval-ms must be at least 100."
                );
            }

            String configuredEndpoint = System.getProperty(
                    "seshat.agent.heartbeat-endpoint"
            );

            URI endpoint = configuredEndpoint == null
                    ? queue.endpoint().resolve("../agents/heartbeat")
                    : URI.create(configuredEndpoint);

            if (endpoint.getHost() == null
                    || (!"http".equalsIgnoreCase(endpoint.getScheme())
                    && !"https".equalsIgnoreCase(endpoint.getScheme()))) {
                throw new IllegalArgumentException(
                        "Heartbeat endpoint must be an HTTP or HTTPS URL."
                );
            }

            client = HttpClient.newBuilder()
                    .connectTimeout(Duration.ofSeconds(2))
                    .followRedirects(HttpClient.Redirect.NEVER)
                    .version(HttpClient.Version.HTTP_1_1)
                    .build();

            activeClient = client;

            boolean failureReported = false;

            while (!stopped.get()) {
                try {
                    byte[] body = mapper.writeValueAsBytes(snapshot());

                    var request = HttpRequest.newBuilder(endpoint)
                            .timeout(Duration.ofSeconds(2))
                            .header("Content-Type", "application/json")
                            .POST(HttpRequest.BodyPublishers.ofByteArray(body))
                            .build();

                    var response = client.send(
                            request,
                            HttpResponse.BodyHandlers.discarding()
                    );

                    if (response.statusCode() != 204) {
                        throw new IOException(
                                "Heartbeat returned HTTP "
                                        + response.statusCode()
                        );
                    }

                    failureReported = false;

                } catch (InterruptedException error) {
                    Thread.currentThread().interrupt();
                    break;

                } catch (IOException | RuntimeException error) {
                    if (!stopped.get() && !failureReported) {
                        log.log(
                                System.Logger.Level.WARNING,
                                "Heartbeat failed; log processing continues. "
                                        + error
                        );
                    }

                    failureReported = true;
                }

                if (stopped.get()) {
                    break;
                }

                try {
                    Thread.sleep(intervalMs);

                } catch (InterruptedException error) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }

        } catch (RuntimeException error) {
            if (!stopped.get()) {
                log.log(
                        System.Logger.Level.ERROR,
                        "Heartbeat reporter stopped; log processing continues.",
                        error
                );
            }

        } finally {
            if (client != null) {
                client.shutdownNow();
            }

            activeClient = null;
        }
    }

    private AgentHeartbeatRequest snapshot() {
        Double processCpuLoad = null;
        Double systemCpuLoad = null;
        Long heapUsedBytes = null;
        Long heapMaxBytes = null;

        try {
            var operatingSystem =
                    ManagementFactory.getOperatingSystemMXBean();

            if (operatingSystem
                    instanceof com.sun.management.OperatingSystemMXBean os) {
                double processLoad = os.getProcessCpuLoad();
                double systemLoad = os.getCpuLoad();

                // 첫 CPU 표본은 관측 구간이 명확하지 않으므로 사용하지 않는다.
                if (cpuPrimed) {
                    processCpuLoad = validLoad(processLoad);
                    systemCpuLoad = validLoad(systemLoad);
                }

                cpuPrimed = true;
            }

        } catch (RuntimeException ignored) {
            // 지원하지 않는 운영체제 지표는 null로 유지한다.
        }

        try {
            var heap = ManagementFactory.getMemoryMXBean()
                    .getHeapMemoryUsage();

            if (heap.getUsed() >= 0) {
                heapUsedBytes = heap.getUsed();
            }

            if (heap.getMax() > 0) {
                heapMaxBytes = heap.getMax();
            }

        } catch (RuntimeException ignored) {
            // 지원하지 않는 메모리 지표는 null로 유지한다.
        }

        int pendingCount;
        long pendingBytes;

        synchronized (queue) {
            pendingCount = queue.pendingBatchCount();
            pendingBytes = queue.pendingBytes();
        }

        var modeSnapshot = modeState.snapshot();
        HostMemory hostMemory = HostMemory.read();
        return new AgentHeartbeatRequest(
                queue.runId(),
                queue.sourceId(),
                instanceId,
                modeSnapshot.appliedMode(),
                processCpuLoad,
                systemCpuLoad,
                heapUsedBytes,
                heapMaxBytes,
                hostMemory == null ? null : hostMemory.usedBytes(),
                hostMemory == null ? null : hostMemory.totalBytes(),
                pendingCount,
                pendingBytes,
                maxPendingBatches,
                maxPendingBytes
        );
    }

    @Override
    public void close() {
        if (!stopped.compareAndSet(false, true)) {
            return;
        }

        worker.interrupt();

        HttpClient client = activeClient;

        if (client != null) {
            client.shutdownNow();
        }

        try {
            if (worker.isAlive()) {
                worker.join(Duration.ofSeconds(3));
            }

        } catch (InterruptedException error) {
            Thread.currentThread().interrupt();
        }
    }
}
