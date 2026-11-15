package com.seshat.control;

import com.seshat.control.AutoModeEvaluator.Sample;
import com.seshat.control.AutoModeEvaluator.Samples;
import com.seshat.control.AutoModePolicy.Thresholds;
import com.seshat.control.dto.AgentControlMessages.CreateCommandRequest;
import com.seshat.control.dto.AgentControlMessages.SyncRequest;
import com.seshat.control.dto.AgentControlMessages.Target;
import com.seshat.monitoring.AgentStatusService;
import com.seshat.monitoring.SystemStatusService;
import com.seshat.monitoring.dto.AgentHeartbeatRequest;
import org.junit.jupiter.api.Test;
import org.springframework.mock.env.MockEnvironment;

import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static com.seshat.control.dto.AgentControlMessages.CommandPhase.PENDING_APPLY;
import static com.seshat.ingestion.dto.ParsingMode.AGENT;
import static com.seshat.ingestion.dto.ParsingMode.CENTRAL;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;

class AutoModeControlTest {

    private final AtomicLong clock = new AtomicLong();
    private final Samples offload = new Samples(new Sample(0.90, 600), new Sample(0.20, 450));
    private final AtomicReference<Samples> samples = new AtomicReference<>(offload);
    private final Target target = new Target("run-auto", "source-1", UUID.randomUUID());
    private final AgentControlService service = new AgentControlService(
            20_000L,
            clock::get,
            new AutoModeEvaluator(
                    new AutoModePolicy(new Thresholds(
                            0.40, 0.80, 0.45, 0.85, Duration.ofSeconds(1)
                    )),
                    ignored -> samples.get()
            )
    );

    @Test
    void 자동_명령의_재전달은_표본_신선도에_따르고_유지_시간은_적용_확인부터_센다() {
        var initial = new SyncRequest(target, null, null, CENTRAL);
        assertNull(service.sync(initial).command());

        clock.set(TimeUnit.SECONDS.toNanos(1));
        var issued = service.sync(initial);
        var command = issued.command();
        assertNotNull(command);
        assertEquals(AGENT, command.mode());
        assertEquals(450L, issued.validForMs());
        assertEquals("CENTRAL_BUSY_AGENT_AVAILABLE", service.status(target).commandReason());

        var received = new SyncRequest(target, command.id(), null, CENTRAL);
        samples.set(new Samples(new Sample(0.90, 0), new Sample(0.20, 450)));
        assertNull(service.sync(received).command());
        assertEquals(PENDING_APPLY, service.status(target).phase());

        samples.set(offload);
        assertEquals(command, service.sync(received).command());

        clock.set(TimeUnit.SECONDS.toNanos(5));
        samples.set(new Samples(new Sample(0.20, 600), new Sample(0.90, 450)));
        var applied = new SyncRequest(target, command.id(), command.id(), AGENT);
        assertNull(service.sync(applied).command());

        clock.set(TimeUnit.SECONDS.toNanos(6));
        var reverse = service.sync(applied).command();
        assertNotNull(reverse);
        assertEquals(CENTRAL, reverse.mode());
        assertEquals(command.id().commandVersion() + 1L, reverse.id().commandVersion());
    }

    @Test
    void 시연_CPU는_일치하는_에이전트의_유효한_하트비트에만_적용한다() {
        var environment = new MockEnvironment()
                .withProperty("seshat.demo.enabled", "true")
                .withProperty("seshat.demo.auto.enabled", "true");
        var systems = mock(SystemStatusService.class);
        var agents = new AgentStatusService(20_000);
        var evaluator = new AutoModeEvaluator(systems, agents, environment);
        var otherInstance = new Target(target.runId(), target.sourceId(), UUID.randomUUID());
        var heldFor = Duration.ofSeconds(4);

        evaluator.setDemoCpu(target, 0.90, 0.20);
        assertEquals(CENTRAL, evaluator.evaluate(target, CENTRAL, heldFor).decision().nextMode());

        agents.receive(new AgentHeartbeatRequest(target.runId(), target.sourceId(), target.instanceId(),
                CENTRAL, null, 0.60, null, null, null, null, 0, 0L, 16, 1024L));
        assertEquals(AGENT, evaluator.evaluate(target, CENTRAL, heldFor).decision().nextMode());
        assertEquals(CENTRAL, evaluator.evaluate(otherInstance, CENTRAL, heldFor).decision().nextMode());

        evaluator.setDemoCpu(target, 0.20, 0.90);
        assertEquals(CENTRAL, evaluator.evaluate(target, AGENT, heldFor).decision().nextMode());
        evaluator.clearDemoCpu(target);
        assertEquals(AGENT, evaluator.evaluate(target, AGENT, heldFor).decision().nextMode());
        verifyNoInteractions(systems);
    }

    @Test
    void 수동_명령이_대기_중이면_자동_전환_조건이_충족돼도_먼저_전달한다() {
        var initial = new SyncRequest(target, null, null, CENTRAL);
        service.sync(initial);
        var manual = service.issue(new CreateCommandRequest(target, UUID.randomUUID(), CENTRAL));

        clock.set(TimeUnit.SECONDS.toNanos(2));
        assertEquals(manual, service.sync(initial).command());
        assertNull(service.status(target).commandReason());
    }
}