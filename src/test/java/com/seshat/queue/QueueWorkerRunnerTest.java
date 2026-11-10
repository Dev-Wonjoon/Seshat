package com.seshat.queue;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class QueueWorkerRunnerTest {

    private final QueueRecoveryService recovery =
            mock(QueueRecoveryService.class);

    private final QueueWorker worker =
            mock(QueueWorker.class);

    private final ScheduledExecutorService executor =
            mock(ScheduledExecutorService.class);

    private final QueueWorkerRunner runner =
            new QueueWorkerRunner(recovery, worker, 1000, executor);

    @Test
    void 회수가_끝난_뒤_작업자를_등록한다() {
        when(recovery.recoverInterrupted()).thenReturn(2);

        runner.run(null);

        var task = ArgumentCaptor.forClass(Runnable.class);
        var order = inOrder(recovery, executor);

        order.verify(recovery).recoverInterrupted();
        order.verify(executor).scheduleWithFixedDelay(
                task.capture(),
                eq(0L),
                eq(1000L),
                eq(TimeUnit.MILLISECONDS)
        );

        // 등록만으로 mock executor가 작업을 실행하지는 않는다.
        verifyNoInteractions(worker);

        task.getValue().run();

        verify(worker).runOnce();
    }

    @Test
    void 회수에_실패하면_작업자를_등록하지_않는다() {
        var cause = new IllegalStateException("Recovery failed.");

        when(recovery.recoverInterrupted()).thenThrow(cause);

        var thrown = assertThrows(
                IllegalStateException.class,
                () -> runner.run(null)
        );

        assertSame(cause, thrown);
        verifyNoInteractions(worker, executor);
    }

    @Test
    void 작업_실패가_다음_실행을_막지_않는다() {
        when(worker.runOnce())
                .thenThrow(new IllegalStateException("Storage failed."))
                .thenReturn(0);

        runner.run(null);

        var task = ArgumentCaptor.forClass(Runnable.class);

        verify(executor).scheduleWithFixedDelay(
                task.capture(),
                eq(0L),
                eq(1000L),
                eq(TimeUnit.MILLISECONDS)
        );

        assertDoesNotThrow(() -> task.getValue().run());
        assertDoesNotThrow(() -> task.getValue().run());

        verify(worker, times(2)).runOnce();
    }

    @Test
    void 종료할_때_현재_작업의_완료를_기다린다()
            throws InterruptedException {
        when(executor.awaitTermination(30, TimeUnit.SECONDS))
                .thenReturn(true);

        runner.destroy();

        var order = inOrder(executor);

        order.verify(executor).shutdown();
        order.verify(executor).awaitTermination(30, TimeUnit.SECONDS);

        verify(executor, never()).shutdownNow();
    }

    @Test
    void 종료_대기_시간을_넘기면_인터럽트를_요청한다()
            throws InterruptedException {
        when(executor.awaitTermination(30, TimeUnit.SECONDS))
                .thenReturn(false);

        runner.destroy();

        var order = inOrder(executor);

        order.verify(executor).shutdown();
        order.verify(executor).awaitTermination(30, TimeUnit.SECONDS);
        order.verify(executor).shutdownNow();
    }

    @Test
    void 실행_간격은_양수여야_한다() {
        assertThrows(
                IllegalArgumentException.class,
                () -> new QueueWorkerRunner(
                        recovery, worker, 0, executor
                )
        );
    }
}