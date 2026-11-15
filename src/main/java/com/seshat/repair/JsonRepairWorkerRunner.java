package com.seshat.repair;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@Component
@ConditionalOnProperty(prefix = "seshat.repair", name = {"enabled", "scheduler-enabled"},
        havingValue = "true")
public class JsonRepairWorkerRunner implements ApplicationRunner, DisposableBean {

    private static final Logger log = LoggerFactory.getLogger(JsonRepairWorkerRunner.class);

    private final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor(
            Thread.ofPlatform().name("seshat-json-repair").daemon(true).factory());
    private final JsonRepairWorker worker;
    private final long delayMillis;

    public JsonRepairWorkerRunner(
            JsonRepairWorker worker,
            @Value("${seshat.repair.poll-delay-ms:1000}") long delayMillis
    ) {
        if (delayMillis <= 0) {
            throw new IllegalArgumentException("JSON 복구 작업 실행 간격은 양수여야 합니다.");
        }
        this.worker = worker;
        this.delayMillis = delayMillis;
    }

    @Override
    public void run(ApplicationArguments args) {
        executor.scheduleWithFixedDelay(this::poll, 0, delayMillis, TimeUnit.MILLISECONDS);
    }

    private void poll() {
        try {
            worker.runOnce();
        } catch (RuntimeException exception) {
            // 작업자 한 번의 오류로 이후 폴링이 중단되지 않도록 한다.
            log.error("JSON 복구 작업을 실행하지 못했습니다. 오류 유형={}",
                    exception.getClass().getSimpleName());
        }
    }

    @Override
    public void destroy() {
        executor.shutdownNow();
    }
}
