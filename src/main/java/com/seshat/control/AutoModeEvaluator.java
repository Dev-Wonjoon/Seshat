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
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.Objects;
import java.util.function.Function;

@Component
@ConditionalOnProperty(prefix = "seshat.control.auto", name = "enabled", havingValue = "true")
public final class AutoModeEvaluator {

    private static final String PREFIX = "seshat.control.auto.";

    private final AutoModePolicy policy;
    private final Function<Target, Samples> samples;

    @Autowired
    public AutoModeEvaluator(
            SystemStatusService systems,
            AgentStatusService agents,
            Environment environment
    ) {
        this(
                new AutoModePolicy(new Thresholds(
                        environment.getRequiredProperty(PREFIX + "central-low", Double.class),
                        environment.getRequiredProperty(PREFIX + "central-high", Double.class),
                        environment.getRequiredProperty(PREFIX + "agent-low", Double.class),
                        environment.getRequiredProperty(PREFIX + "agent-high", Double.class),
                        Duration.ofMillis(
                                environment.getRequiredProperty(PREFIX + "minimum-hold-ms", Long.class)
                        )
                )),
                target -> readSamples(systems, agents, target)
        );
    }

    AutoModeEvaluator(AutoModePolicy policy, Function<Target, Samples> samples) {
        this.policy = Objects.requireNonNull(policy);
        this.samples = Objects.requireNonNull(samples);
    }

    Evaluation evaluate(Target target, ParsingMode appliedMode, Duration heldFor) {
        Samples current = samples.apply(target);

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
                policy.evaluate(input),
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