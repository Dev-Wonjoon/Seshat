package com.seshat.monitoring;

import com.seshat.monitoring.dto.SystemStatusResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.lang.management.ManagementFactory;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.DoubleSupplier;

@Service
public class SystemStatusService
        implements InitializingBean, DisposableBean {

    private static final Logger log =
            LoggerFactory.getLogger(SystemStatusService.class);

    private final UUID instanceId = UUID.randomUUID();

    private final long sampleIntervalMs;
    private final long staleAfterMs;

    private final ScheduledExecutorService executor;

    private volatile Snapshot latest;

    private long sampleSequence;
    private boolean cpuPrimed;

    public SystemStatusService(
            @Value("${seshat.monitoring.system-sample-interval-ms:5000}")
            long sampleIntervalMs
    ) {
        if (sampleIntervalMs < 100 || sampleIntervalMs > 60_000) {
            throw new IllegalArgumentException(
                    "system-sample-interval-ms must be between 100 and 60000."
            );
        }

        this.sampleIntervalMs = sampleIntervalMs;
        this.staleAfterMs = sampleIntervalMs * 3;

        this.executor = Executors.newSingleThreadScheduledExecutor(
                Thread.ofPlatform()
                        .name("seshat-system-metrics")
                        .daemon(true)
                        .factory()
        );
    }

    private static Double readLoad(DoubleSupplier reader) {
        try {
            double value = reader.getAsDouble();

            return Double.isFinite(value) && value >= 0 && value <= 1
                    ? value
                    : null;

        } catch (RuntimeException ignored) {
            return null;
        }
    }

    @Override
    public void afterPropertiesSet() {
        sampleSafely();

        executor.scheduleWithFixedDelay(
                this::sampleSafely,
                sampleIntervalMs,
                sampleIntervalMs,
                TimeUnit.MILLISECONDS
        );
    }

    public SystemStatusResponse snapshot() {
        Snapshot current = latest;

        Long sampleAgeMs = current == null
                ? null
                : Math.max(
                0L,
                TimeUnit.NANOSECONDS.toMillis(
                        System.nanoTime() - current.sampledAtNanos()
                )
        );

        String metricsStatus = "STARTING";

        if (sampleAgeMs != null) {
            metricsStatus = sampleAgeMs >= staleAfterMs
                    ? "STALE"
                    : "FRESH";
        }

        return new SystemStatusResponse(
                instanceId,
                metricsStatus,
                System.currentTimeMillis(),
                sampleIntervalMs,
                staleAfterMs,
                current == null ? 0L : current.sequence(),
                current == null ? null : current.sampledAtMs(),
                sampleAgeMs,
                current == null ? null : current.uptimeMs(),
                current == null ? null : current.processCpuLoad(),
                current == null ? null : current.systemCpuLoad(),
                current == null ? null : current.heapUsedBytes(),
                current == null ? null : current.heapMaxBytes()
        );
    }

    private void sampleSafely() {
        try {
            sample();

        } catch (RuntimeException exception) {
            // 이전 표본과 해당 표본의 수집 시각을 그대로 유지한다.
            log.warn("System metrics sampling failed.", exception);
        }
    }

    private void sample() {
        Double processCpuLoad = null;
        Double systemCpuLoad = null;
        Long heapUsedBytes = null;
        Long heapMaxBytes = null;

        try {
            var operatingSystem =
                    ManagementFactory.getOperatingSystemMXBean();

            if (operatingSystem
                    instanceof com.sun.management.OperatingSystemMXBean os) {
                Double processLoad = readLoad(os::getProcessCpuLoad);
                Double systemLoad = readLoad(os::getCpuLoad);

                // 첫 CPU 표본은 관측 구간이 명확하지 않으므로 사용하지 않는다.
                if (cpuPrimed) {
                    processCpuLoad = processLoad;
                    systemCpuLoad = systemLoad;
                }

                cpuPrimed = true;
            }

        } catch (RuntimeException ignored) {
            // 지원하지 않는 CPU 지표는 null로 유지한다.
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

        long uptimeMs = ManagementFactory.getRuntimeMXBean()
                .getUptime();

        latest = new Snapshot(
                ++sampleSequence,
                System.currentTimeMillis(),
                System.nanoTime(),
                uptimeMs,
                processCpuLoad,
                systemCpuLoad,
                heapUsedBytes,
                heapMaxBytes
        );
    }

    @Override
    public void destroy() {
        executor.shutdownNow();

        try {
            if (!executor.awaitTermination(3, TimeUnit.SECONDS)) {
                log.warn("System metrics sampler did not stop within 3 seconds.");
            }

        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
        }
    }

    private record Snapshot(
            long sequence,
            long sampledAtMs,
            long sampledAtNanos,
            long uptimeMs,
            Double processCpuLoad,
            Double systemCpuLoad,
            Long heapUsedBytes,
            Long heapMaxBytes
    ) {
    }
}