package com.seshat.control;

import com.seshat.control.AgentControlService.ControlException;
import com.seshat.control.dto.AgentControlMessages.CreateCommandRequest;
import com.seshat.control.dto.AgentControlMessages.SyncRequest;
import com.seshat.control.dto.AgentControlMessages.Target;
import org.junit.jupiter.api.Test;

import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static com.seshat.control.dto.AgentControlMessages.CommandPhase.*;
import static com.seshat.ingestion.dto.ParsingMode.*;
import static org.junit.jupiter.api.Assertions.*;

class AgentControlServiceTest {

    private final AtomicLong clock = new AtomicLong();
    private final AgentControlService service = new AgentControlService(20L, clock::get);
    private final Target target = new Target("run-1", "source-1", UUID.randomUUID());

    @Test
    void 최신_명령을_수신하고_이전_명령을_적용하면_최신_명령은_적용_대기다() {
        var initial = new SyncRequest(target, null, null, CENTRAL);
        assertNull(service.sync(initial).command());

        var first = service.issue(new CreateCommandRequest(target, UUID.randomUUID(), AGENT));
        var second = service.issue(new CreateCommandRequest(target, UUID.randomUUID(), AGENT));

        assertEquals(PENDING_DELIVERY, service.status(target).phase());
        assertEquals(second, service.sync(initial).command());
        assertEquals(PENDING_DELIVERY, service.status(target).phase());

        var between = new SyncRequest(target, second.id(), first.id(), AGENT);
        service.sync(between);
        assertEquals(PENDING_APPLY, service.status(target).phase());

        service.sync(new SyncRequest(target, second.id(), second.id(), AGENT));
        assertEquals(APPLIED, service.status(target).phase());

        service.sync(between);
        assertEquals(APPLIED, service.status(target).phase());
    }

    @Test
    void 운영자_재전송은_최초_명령을_반환하고_같은_ID의_다른_내용은_거부한다() {
        service.sync(new SyncRequest(target, null, null, CENTRAL));
        var request = new CreateCommandRequest(target, UUID.randomUUID(), AGENT);
        var first = service.issue(request);
        var latest = service.issue(new CreateCommandRequest(target, UUID.randomUUID(), CENTRAL));

        assertEquals(first, service.issue(request));
        assertEquals(latest, service.status(target).command());
        assertEquals(first.id().commandVersion() + 1L, latest.id().commandVersion());

        var conflict = new CreateCommandRequest(target, request.requestId(), CENTRAL);
        assertEquals("REQUEST_ID_CONFLICT",
                assertThrows(ControlException.class, () -> service.issue(conflict)).code());
    }

    @Test
    void 기존_세션이_만료된_후_새_인스턴스가_명령_없이_등록된다() {
        service.sync(new SyncRequest(target, null, null, CENTRAL));
        service.issue(new CreateCommandRequest(target, UUID.randomUUID(), AGENT));

        var replacement = new Target(target.runId(), target.sourceId(), UUID.randomUUID());
        var replacementSync = new SyncRequest(replacement, null, null, CENTRAL);

        assertEquals("CONTROL_SESSION_CONFLICT",
                assertThrows(ControlException.class, () -> service.sync(replacementSync)).code());

        clock.set(TimeUnit.MILLISECONDS.toNanos(20L));
        assertFalse(service.status(target).active());
        assertNull(service.sync(replacementSync).command());
        assertTrue(service.status(replacement).active());

        var fresh = service.issue(new CreateCommandRequest(replacement, UUID.randomUUID(), AGENT));
        assertEquals(1L, fresh.id().commandVersion());
    }
}