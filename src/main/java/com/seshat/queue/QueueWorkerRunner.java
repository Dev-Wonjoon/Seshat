package com.seshat.queue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@Component
@ConditionalOnProperty(
        prefix = "seshat.queue",
        name = "worker-enabled",
        havingValue = "true"
)
public class QueueWorkerRunner implements ApplicationRunner, DisposableBean {

    private static final Logger log =
            LoggerFactory.getLogger(QueueWorkerRunner.class);

    private final QueueRecoveryService recovery;
    private final QueueWorker worker;
    private final long delayMillis;
    private final ScheduledExecutorService executor;

    @Autowired
    public QueueWorkerRunner(
            QueueRecoveryService recovery,
            QueueWorker worker,
            @Value("${seshat.queue.worker-delay-ms:1000}")
            long delayMillis
    ) {
        this(
                recovery,
                worker,
                delayMillis,
                Executors.newSingleThreadScheduledExecutor(
                        Thread.ofPlatform()
                                .name("seshat-queue-worker")
                                .daemon(true)
                                .factory()
                )
        );
    }

    // 테스트에서는 실제 스레드 대신 mock executor를 전달한다.
    QueueWorkerRunner(
            QueueRecoveryService recovery,
            QueueWorker worker,
            long delayMillis,
            ScheduledExecutorService executor
    ) {
        if (delayMillis <= 0) {
            throw new IllegalArgumentException(
                    "Worker delay must be positive."
            );
        }

        this.recovery = recovery;
        this.worker = worker;
        this.delayMillis = delayMillis;
        this.executor = executor;
    }

    @Override
    public void run(ApplicationArguments args) {
        // 실패하면 예외를 전달한다. 작업자는 등록하지 않는다.
        int recovered = recovery.recoverInterrupted();

        log.info("Recovered {} interrupted queue events.", recovered);

        executor.scheduleWithFixedDelay(
                this::processBatch,
                0,
                delayMillis,
                TimeUnit.MILLISECONDS
        );
    }

    private void processBatch() {
        try {
            int sent = worker.runOnce();

            if (sent > 0) {
                log.debug("Queue batch processed: sent={}", sent);
            }
        } catch (RuntimeException exception) {
            // 예외가 스케줄러 밖으로 나가면 후속 실행이 중단될 수 있다.
            log.error("Queue worker execution failed.", exception);
        }
    }

    @Override
    public void destroy() {
        // 이후 주기 실행을 중단하고 현재 실행의 완료를 기다린다.
        executor.shutdown();

        try {
            if (!executor.awaitTermination(30, TimeUnit.SECONDS)) {
                log.warn(
                        "Queue worker did not stop within 30 seconds. "
                                + "Requesting interruption."
                );
                executor.shutdownNow();
            }
        } catch (InterruptedException exception) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
}