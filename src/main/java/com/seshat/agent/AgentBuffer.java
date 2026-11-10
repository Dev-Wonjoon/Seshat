package com.seshat.agent;

import com.seshat.ingestion.dto.LogEventRequest;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

public final class AgentBuffer {

    private final int maxUnacknowledgedEvents;
    private final Semaphore capacity;
    private final ArrayBlockingQueue<Entry> waiting;

    public AgentBuffer(int maxUnacknowledgedEvents) {
        if (maxUnacknowledgedEvents < 1) {
            throw new IllegalArgumentException(
                    "Queue capacity must be positive."
            );
        }

        this.maxUnacknowledgedEvents = maxUnacknowledgedEvents;

        capacity = new Semaphore(maxUnacknowledgedEvents);
        waiting = new ArrayBlockingQueue<>(maxUnacknowledgedEvents);
    }

    public void submit(Supplier<LogEventRequest> factory)
            throws InterruptedException {

        // 파싱 결과를 만들기 전에 용량부터 확보한다.
        capacity.acquire();
        boolean queued = false;

        try {
            long admittedAt = System.nanoTime();
            var entry = new Entry(factory.get(), admittedAt);

            waiting.put(entry);
            queued = true;
        } finally {
            if (!queued) {
                capacity.release();
            }
        }
    }

    public Entry take() throws InterruptedException {
        return waiting.take();
    }

    public Entry poll(long timeoutNanos)
            throws InterruptedException {
        return waiting.poll(
                Math.max(0, timeoutNanos),
                TimeUnit.NANOSECONDS
        );
    }

    public final class Entry {

        private final LogEventRequest event;
        private final long admittedAtNanos;
        private final AtomicBoolean acknowledged =
                new AtomicBoolean();

        private Entry(
                LogEventRequest event,
                long admittedAtNanos
        ) {
            this.event = event;
            this.admittedAtNanos = admittedAtNanos;
        }

        public LogEventRequest event() {
            return event;
        }

        public long admittedAtNanos() {
            return admittedAtNanos;
        }

        public void acknowledge() {
            // 중복 호출로 용량이 늘어나는 것을 방지한다.
            if (acknowledged.compareAndSet(false, true)) {
                capacity.release();
            }
        }
    }

    public int unacknowledgedCount() {
        return maxUnacknowledgedEvents - capacity.availablePermits();
    }
}