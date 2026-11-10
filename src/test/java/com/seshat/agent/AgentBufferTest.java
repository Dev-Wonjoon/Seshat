package com.seshat.agent;

import com.seshat.ingestion.dto.LogEventRequest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

@Timeout(5)
class AgentBufferTest {

    private static LogEventRequest event(String message) {
        return new LogEventRequest(
                UUID.randomUUID(),
                "{\"message\":\"" + message + "\"}",
                null,
                null,
                null
        );
    }

    @Test
    void 큐에서_꺼내도_중앙_확인_전에는_용량을_반환하지_않는다()
            throws InterruptedException {

        var buffer = new AgentBuffer(1);
        var event = event("first");

        buffer.submit(() -> event);
        var entry = buffer.take();

        assertSame(event, entry.event());
        assertNull(buffer.poll(0));
        assertEquals(1, buffer.unacknowledgedCount());

        entry.acknowledge();

        assertEquals(0, buffer.unacknowledgedCount());
    }

    @Test
    void 이전_이벤트의_중복_확인이_새_이벤트의_용량을_반환하지_않는다()
            throws InterruptedException {

        var buffer = new AgentBuffer(1);

        buffer.submit(() -> event("first"));
        var first = buffer.take();
        first.acknowledge();

        buffer.submit(() -> event("second"));
        var second = buffer.take();

        // 이전 이벤트의 확인 처리가 늦게 다시 호출된 상황.
        first.acknowledge();

        assertEquals(1, buffer.unacknowledgedCount());

        second.acknowledge();
        second.acknowledge();

        assertEquals(0, buffer.unacknowledgedCount());
    }

    @Test
    void 이벤트_생성에_실패하면_확보한_용량을_반환한다()
            throws InterruptedException {

        var buffer = new AgentBuffer(1);
        var expected = new IllegalStateException(
                "Event creation failed."
        );

        var actual = assertThrows(
                IllegalStateException.class,
                () -> buffer.submit(() -> {
                    throw expected;
                })
        );

        assertSame(expected, actual);
        assertEquals(0, buffer.unacknowledgedCount());
        assertNull(buffer.poll(0));

        // 실패 이후에도 다음 이벤트를 받을 수 있어야 한다.
        buffer.submit(() -> event("next"));
        var next = buffer.take();

        assertEquals(1, buffer.unacknowledgedCount());

        next.acknowledge();

        assertEquals(0, buffer.unacknowledgedCount());
    }

    @Test
    void 전송_중인_이벤트가_있으면_확인까지_다음_생성을_대기한다()
            throws Exception {

        var buffer = new AgentBuffer(1);

        buffer.submit(() -> event("first"));
        var first = buffer.take();

        // 대기 큐는 비었지만 첫 이벤트는 아직 미확인 상태다.
        var attempted = new CountDownLatch(1);
        var factoryCalls = new AtomicInteger();
        var secondEvent = event("second");

        var executor = Executors.newSingleThreadExecutor();

        try {
            var submitted = executor.submit(() -> {
                attempted.countDown();

                buffer.submit(() -> {
                    factoryCalls.incrementAndGet();
                    return secondEvent;
                });

                return null;
            });

            assertTrue(attempted.await(1, TimeUnit.SECONDS));

            assertThrows(
                    TimeoutException.class,
                    () -> submitted.get(
                            200,
                            TimeUnit.MILLISECONDS
                    )
            );

            assertEquals(0, factoryCalls.get());
            assertEquals(1, buffer.unacknowledgedCount());

            // 중앙 확인을 받으면 대기 중인 등록이 진행된다.
            first.acknowledge();
            submitted.get(1, TimeUnit.SECONDS);

            assertEquals(1, factoryCalls.get());

            var second = buffer.take();

            assertSame(secondEvent, second.event());
            assertEquals(1, buffer.unacknowledgedCount());

            second.acknowledge();

            assertEquals(0, buffer.unacknowledgedCount());
        } finally {
            // 검증 실패 시에도 대기 중인 작업을 해제한다.
            executor.shutdownNow();
        }
    }
}