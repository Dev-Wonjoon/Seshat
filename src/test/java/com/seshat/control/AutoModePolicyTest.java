package com.seshat.control;

import com.seshat.control.AutoModePolicy.CpuObservation;
import com.seshat.control.AutoModePolicy.Decision;
import com.seshat.control.AutoModePolicy.Input;
import com.seshat.control.AutoModePolicy.Thresholds;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static com.seshat.control.AutoModePolicy.Reason.*;
import static com.seshat.ingestion.dto.ParsingMode.*;
import static org.junit.jupiter.api.Assertions.assertEquals;

class AutoModePolicyTest {

    private static final Duration HOLD = Duration.ofSeconds(30);
    private final AutoModePolicy policy =
            new AutoModePolicy(new Thresholds(0.40, 0.80, 0.45, 0.85, HOLD));

    @Test
    void 방향별_임계값을_충족하면_여유가_있는_쪽으로_전환한다() {
        assertEquals(
                new Decision(AGENT, CENTRAL_BUSY_AGENT_AVAILABLE),
                policy.evaluate(new Input(CENTRAL, true, false, HOLD, fresh(0.80), fresh(0.45)))
        );
        assertEquals(
                new Decision(CENTRAL, AGENT_BUSY_CENTRAL_AVAILABLE),
                policy.evaluate(new Input(AGENT, true, false, HOLD, fresh(0.40), fresh(0.85)))
        );
    }

    @Test
    void 양쪽이_바쁘거나_부하가_중간이면_현재_모드를_유지한다() {
        assertEquals(
                new Decision(CENTRAL, KEEP_CURRENT_MODE),
                policy.evaluate(new Input(CENTRAL, true, false, HOLD, fresh(0.90), fresh(0.90)))
        );
        assertEquals(
                new Decision(AGENT, KEEP_CURRENT_MODE),
                policy.evaluate(new Input(AGENT, true, false, HOLD, fresh(0.55), fresh(0.60)))
        );
    }

    @Test
    void 제어_단절과_명령_대기_중에는_전환하지_않는다() {
        assertEquals(
                new Decision(CENTRAL, CONTROL_INACTIVE),
                policy.evaluate(new Input(CENTRAL, false, false, HOLD, fresh(0.90), fresh(0.20)))
        );
        assertEquals(
                new Decision(CENTRAL, COMMAND_PENDING),
                policy.evaluate(new Input(CENTRAL, true, true, HOLD, fresh(0.90), fresh(0.20)))
        );
    }

    @Test
    void 오래된_표본과_측정_불가_값에서는_전환하지_않는다() {
        assertEquals(
                new Decision(CENTRAL, METRICS_STALE),
                policy.evaluate(new Input(
                        CENTRAL, true, false, HOLD, fresh(0.90), new CpuObservation(false, 0.20)
                ))
        );
        assertEquals(
                new Decision(CENTRAL, CPU_UNAVAILABLE),
                policy.evaluate(new Input(CENTRAL, true, false, HOLD, fresh(0.90), fresh(null)))
        );
    }

    @Test
    void 최소_유지_시간_경계에_도달한_뒤에만_전환한다() {
        var central = fresh(0.90);
        var agent = fresh(0.20);

        assertEquals(
                new Decision(CENTRAL, MINIMUM_HOLD),
                policy.evaluate(new Input(CENTRAL, true, false, HOLD.minusMillis(1), central, agent))
        );
        assertEquals(
                new Decision(AGENT, CENTRAL_BUSY_AGENT_AVAILABLE),
                policy.evaluate(new Input(CENTRAL, true, false, HOLD, central, agent))
        );
    }

    private static CpuObservation fresh(Double load) {
        return new CpuObservation(true, load);
    }
}