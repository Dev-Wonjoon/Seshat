package com.seshat.control;

import com.seshat.ingestion.dto.ParsingMode;

import java.time.Duration;
import java.util.Objects;

public final class AutoModePolicy {

    private final Thresholds thresholds;

    public AutoModePolicy(Thresholds thresholds) {
        this.thresholds = Objects.requireNonNull(thresholds);
    }

    public Decision evaluate(Input input) {
        ParsingMode current = input.appliedMode();

        if (!input.controlActive()) {
            return new Decision(current, Reason.CONTROL_INACTIVE);
        }
        if (input.commandPending()) {
            return new Decision(current, Reason.COMMAND_PENDING);
        }
        if (!input.central().fresh() || !input.agent().fresh()) {
            return new Decision(current, Reason.METRICS_STALE);
        }

        Double centralLoad = input.central().systemCpuLoad();
        Double agentLoad = input.agent().systemCpuLoad();
        if (!validCpuLoad(centralLoad) || !validCpuLoad(agentLoad)) {
            return new Decision(current, Reason.CPU_UNAVAILABLE);
        }
        if (input.heldFor().compareTo(thresholds.minimumHold()) < 0) {
            return new Decision(current, Reason.MINIMUM_HOLD);
        }

        if (current == ParsingMode.CENTRAL
                && centralLoad >= thresholds.centralHigh()
                && agentLoad <= thresholds.agentLow()) {
            return new Decision(ParsingMode.AGENT, Reason.CENTRAL_BUSY_AGENT_AVAILABLE);
        }

        if (current == ParsingMode.AGENT
                && agentLoad >= thresholds.agentHigh()
                && centralLoad <= thresholds.centralLow()) {
            return new Decision(ParsingMode.CENTRAL, Reason.AGENT_BUSY_CENTRAL_AVAILABLE);
        }

        return new Decision(current, Reason.KEEP_CURRENT_MODE);
    }

    private static boolean validCpuLoad(Double load) {
        return load != null && load >= 0.0 && load <= 1.0;
    }

    public record Thresholds(
            double centralLow,
            double centralHigh,
            double agentLow,
            double agentHigh,
            Duration minimumHold
    ) {
        public Thresholds {
            if (!validCpuLoad(centralLow) || !validCpuLoad(centralHigh)
                    || !validCpuLoad(agentLow) || !validCpuLoad(agentHigh)
                    || centralLow >= centralHigh || agentLow >= agentHigh) {
                throw new IllegalArgumentException(
                        "CPU 임계값은 0~1 범위이며 낮은 임계값이 높은 임계값보다 작아야 합니다."
                );
            }
            Objects.requireNonNull(minimumHold);
            if (minimumHold.isZero() || minimumHold.isNegative()) {
                throw new IllegalArgumentException("최소 유지 시간은 양수여야 합니다.");
            }
        }
    }

    public record CpuObservation(boolean fresh, Double systemCpuLoad) {
    }

    public record Input(
            ParsingMode appliedMode,
            boolean controlActive,
            boolean commandPending,
            Duration heldFor,
            CpuObservation central,
            CpuObservation agent
    ) {
        public Input {
            Objects.requireNonNull(appliedMode);
            Objects.requireNonNull(heldFor);
            Objects.requireNonNull(central);
            Objects.requireNonNull(agent);
        }
    }

    public record Decision(ParsingMode nextMode, Reason reason) {
    }

    public enum Reason {
        CONTROL_INACTIVE,
        COMMAND_PENDING,
        METRICS_STALE,
        CPU_UNAVAILABLE,
        MINIMUM_HOLD,
        CENTRAL_BUSY_AGENT_AVAILABLE,
        AGENT_BUSY_CENTRAL_AVAILABLE,
        KEEP_CURRENT_MODE
    }
}