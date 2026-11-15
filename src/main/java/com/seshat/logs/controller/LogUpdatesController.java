package com.seshat.logs.controller;

import com.seshat.logs.service.LogQueryService;
import jakarta.annotation.PreDestroy;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.io.IOException;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

@RestController
public class LogUpdatesController {

    private static final System.Logger LOG = System.getLogger(LogUpdatesController.class.getName());
    private final Set<SseEmitter> subscribers = ConcurrentHashMap.newKeySet();
    private final LogQueryService logs;
    private final java.util.concurrent.ScheduledExecutorService polling =
            Executors.newSingleThreadScheduledExecutor(
                    Thread.ofPlatform().daemon().name("log-updates").factory());
    private String revision;
    private int idleTicks;

    public LogUpdatesController(LogQueryService logs) {
        this.logs = logs;
        polling.scheduleWithFixedDelay(this::broadcast, 0, 1, TimeUnit.SECONDS);
    }

    @GetMapping(value = "/api/logs/updates", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public ResponseEntity<SseEmitter> updates() {
        var emitter = new SseEmitter(0L);
        emitter.onCompletion(() -> subscribers.remove(emitter));
        emitter.onTimeout(() -> subscribers.remove(emitter));
        emitter.onError(error -> subscribers.remove(emitter));
        subscribers.add(emitter);
        send(emitter, SseEmitter.event().name("ready").data("ready").reconnectTime(2000));
        return ResponseEntity.ok().header(HttpHeaders.CACHE_CONTROL, "no-cache")
                .header("X-Accel-Buffering", "no").body(emitter);
    }

    private void broadcast() {
        if (subscribers.isEmpty()) {
            return;
        }
        try {
            String newest = logs.visibleRevision();
            if (!newest.equals(revision)) {
                revision = newest;
                idleTicks = 0;
                subscribers.forEach(emitter -> send(emitter,
                        SseEmitter.event().name("logs").data(newest)));
                return;
            }
        } catch (IOException | RuntimeException error) {
            LOG.log(System.Logger.Level.DEBUG, "로그 갱신 상태를 조회하지 못했습니다.", error);
        }

        if (++idleTicks >= 15) {
            idleTicks = 0;
            subscribers.forEach(emitter -> send(emitter, SseEmitter.event().comment("keepalive")));
        }
    }

    private void send(SseEmitter emitter, SseEmitter.SseEventBuilder event) {
        try {
            emitter.send(event);
        } catch (IOException | IllegalStateException error) {
            subscribers.remove(emitter);
            emitter.complete();
        }
    }

    @PreDestroy
    void close() {
        polling.shutdownNow();
        subscribers.forEach(SseEmitter::complete);
    }
}
