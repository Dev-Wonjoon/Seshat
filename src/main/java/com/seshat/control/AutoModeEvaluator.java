package com.seshat.control;

import com.seshat.control.AutoModePolicy.CpuObservation;
import com.seshat.control.AutoModePolicy.Decision;
import com.seshat.control.AutoModePolicy.Input;
import com.seshat.control.AutoModePolicy.Thresholds;
import com.seshat.control.dto.AgentControlMessages.Target;
import com.seshat.ingestion.dto.ParsingMode;
import com.seshat.monitoring.AgentStatusService;
import com.seshat.monitoring.SystemStatusService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.Objects;
import java.util.function.Function;

@Component
@ConditionalOnExpression("${seshat.control.auto.enabled:false} || "
        + "(${seshat.demo.enabled:false} && ${seshat.demo.auto.enabled:false})")
public final class AutoModeEvaluator {

    private static final String PREFIX = "seshat.control.auto.";

    private final AutoModePolicy policy;
    private final Function<Target, Samples> samples;
    private final AutoModePolicy demoPolicy;
    private final AgentStatusService agents;

    // 시연 스레드와 제어 동기화 스레드가 현재 시연 대상과 CPU 값을 공유한다.
    private volatile DemoSamples demoSamples;

    @Autowired
    public AutoModeEvaluator(
            SystemStatusService systems,
            AgentStatusService agents,
            Environment environment
    ) {
        this.policy = Boolean.TRUE.equals(environment.getProperty(PREFIX + "enabled", Boolean.class))
                ? new AutoModePolicy(new Thresholds(
                        environment.getRequiredProperty(PREFIX + "central-low", Double.class),
                        environment.getRequiredProperty(PREFIX + "central-high", Double.class),
                        environment.getRequiredProperty(PREFIX + "agent-low", Double.class),
                        environment.getRequiredProperty(PREFIX + "agent-high", Double.class),
                        Duration.ofMillis(
                                environment.getRequiredProperty(PREFIX + "minimum-hold-ms", Long.class)
                        )
                )) : null;
        this.samples = target -> readSamples(systems, agents, target);
        this.agents = agents;
        this.demoPolicy = Boolean.TRUE.equals(environment.getProperty("seshat.demo.enabled", Boolean.class))
                && Boolean.TRUE.equals(environment.getProperty("seshat.demo.auto.enabled", Boolean.class))
                ? new AutoModePolicy(new Thresholds(0.40, 0.80, 0.45, 0.85, Duration.ofSeconds(3)))
                : null;
    }

    AutoModeEvaluator(AutoModePolicy policy, Function<Target, Samples> samples) {
        this.policy = Objects.requireNonNull(policy);
        this.samples = Objects.requireNonNull(samples);
        this.demoPolicy = null;
        this.agents = null;
    }

    public boolean demoEnabled() {
        return demoPolicy != null;
    }

    public void setDemoCpu(Target target, double central, double agent) {
        if (!demoEnabled()) {
            throw new IllegalStateException("자동 전환 시연이 꺼져 있습니다.");
        }
        demoSamples = new DemoSamples(target, central, agent);
    }

    public void clearDemoCpu(Target target) {
        var current = demoSamples;
        if (current != null && current.target().equals(target)) {
            demoSamples = null;
        }
    }

    Evaluation evaluate(Target target, ParsingMode appliedMode, Duration heldFor) {
        var demo = demoSamples;
        if (demo != null && demo.target().equals(target)) {
            // CPU 값만 대체하며 대상의 실제 하트비트 유효 기간을 사용한다.
            var heartbeat = agents.controlCpuSample(target.runId(), target.sourceId(), target.instanceId());
            long validForMs = heartbeat.validForMs();
            var simulated = new Samples(new Sample(demo.central(), validForMs), new Sample(demo.agent(), validForMs));
            return evaluate(demoPolicy, simulated, appliedMode, heldFor);
        }
        if (policy == null) {
            return new Evaluation(new Decision(appliedMode, AutoModePolicy.Reason.CONTROL_INACTIVE), 0);
        }
        return evaluate(policy, samples.apply(target), appliedMode, heldFor);
    }

    private Evaluation evaluate(AutoModePolicy activePolicy, Samples current, ParsingMode appliedMode,
                                Duration heldFor) {

        // 유효한 sync 안에서 호출하며, 수동 명령 대기는 호출 전에 처리한다.
        var input = new Input(
                appliedMode,
                true,
                false,
                heldFor,
                current.central().observation(),
                current.agent().observation()
        );

        return new Evaluation(
                activePolicy.evaluate(input),
                Math.min(current.central().validForMs(), current.agent().validForMs())
        );
    }

    private static Samples readSamples(
            SystemStatusService systems,
            AgentStatusService agents,
            Target target
    ) {
        var central = systems.snapshot();
        Long ageMs = central.sampleAgeMs();
        long centralRemainingMs = "FRESH".equals(central.metricsStatus()) && ageMs != null
                ? Math.max(0L, central.staleAfterMs() - ageMs - 1L)
                : 0L;

        var agent = agents.controlCpuSample(
                target.runId(), target.sourceId(), target.instanceId()
        );

        return new Samples(
                new Sample(central.systemCpuLoad(), centralRemainingMs),
                new Sample(agent.load(), agent.validForMs())
        );
    }

    private record DemoSamples(Target target, double central, double agent) {
    }

    record Sample(Double load, long validForMs) {
        CpuObservation observation() {
            return new CpuObservation(validForMs > 0L, load);
        }
    }

    record Samples(Sample central, Sample agent) {
    }

    record Evaluation(Decision decision, long validForMs) {
    }
}