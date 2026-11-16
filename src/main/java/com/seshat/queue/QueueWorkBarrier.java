package com.seshat.queue;

import org.springframework.stereotype.Component;

import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Supplier;

@Component
public final class QueueWorkBarrier {
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock(true);

    public <T> T process(Supplier<T> work) {
        var processing = lock.readLock();
        processing.lock();
        try {
            return work.get();
        } finally {
            processing.unlock();
        }
    }

    public void delete(Runnable work) {
        var deleting = lock.writeLock();
        deleting.lock();
        try {
            work.run();
        } finally {
            deleting.unlock();
        }
    }
}
