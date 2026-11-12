package com.seshat.agent;

import com.seshat.ingestion.dto.ParsingMode;
import org.junit.jupiter.api.Test;
import com.seshat.control.dto.AgentControlMessages.Command;
import com.seshat.control.dto.AgentControlMessages.CommandId;


import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.*;

class AgentModeStateTest {

    @Test
    void 초기_모드는_요청과_적용이_같고_대기_요청이_없다() {
        var state = new AgentModeState(ParsingMode.CENTRAL);

        var snapshot = state.snapshot();

        assertEquals(ParsingMode.CENTRAL, snapshot.requestedMode());
        assertEquals(ParsingMode.CENTRAL, snapshot.appliedMode());
        assertEquals(0L, snapshot.requestedRevision());
        assertEquals(0L, snapshot.appliedRevision());
        assertFalse(snapshot.hasPendingRequest());
    }

    @Test
    void 요청과_배치_예약만으로는_적용_상태가_바뀌지_않는다() {
        var state = new AgentModeState(ParsingMode.CENTRAL);

        state.requestMode(ParsingMode.AGENT);
        var selection = state.selectForNextBatch();

        assertEquals(ParsingMode.AGENT, selection.mode());
        assertEquals(1L, selection.revision());

        var snapshot = state.snapshot();

        assertEquals(ParsingMode.AGENT, snapshot.requestedMode());
        assertEquals(1L, snapshot.requestedRevision());
        assertEquals(ParsingMode.CENTRAL, snapshot.appliedMode());
        assertEquals(0L, snapshot.appliedRevision());
        assertTrue(snapshot.hasPendingRequest());
    }

    @Test
    void 예약한_배치를_적용하면_대기_요청이_해소된다() {
        var state = new AgentModeState(ParsingMode.AGENT);

        state.requestMode(ParsingMode.CENTRAL);
        var selection = state.selectForNextBatch();

        state.markApplied(selection);

        var snapshot = state.snapshot();

        assertEquals(ParsingMode.CENTRAL, snapshot.requestedMode());
        assertEquals(ParsingMode.CENTRAL, snapshot.appliedMode());
        assertEquals(
                snapshot.requestedRevision(),
                snapshot.appliedRevision()
        );
        assertFalse(snapshot.hasPendingRequest());
    }

    @Test
    void 예약_후_최신_요청이_도착해도_기존_예약을_적용하고_최신_요청은_남긴다() {
        var state = new AgentModeState(ParsingMode.CENTRAL);

        state.requestMode(ParsingMode.AGENT);
        var reserved = state.selectForNextBatch();

        // 첫 배치를 준비하는 사이 다음 모드 요청이 도착한 순서를 재현한다.
        state.requestMode(ParsingMode.CENTRAL);
        state.markApplied(reserved);

        var pending = state.snapshot();

        assertEquals(ParsingMode.CENTRAL, pending.requestedMode());
        assertEquals(2L, pending.requestedRevision());
        assertEquals(ParsingMode.AGENT, pending.appliedMode());
        assertEquals(1L, pending.appliedRevision());
        assertTrue(pending.hasPendingRequest());

        // 최신 요청은 다음 배치에 적용한다.
        var next = state.selectForNextBatch();

        assertEquals(ParsingMode.CENTRAL, next.mode());
        assertEquals(2L, next.revision());

        state.markApplied(next);

        var completed = state.snapshot();

        assertEquals(ParsingMode.CENTRAL, completed.appliedMode());
        assertEquals(2L, completed.appliedRevision());
        assertFalse(completed.hasPendingRequest());
    }

    @Test
    void 같은_모드를_반복_요청하면_순번과_적용_상태를_유지한다() {
        var state = new AgentModeState(ParsingMode.CENTRAL);

        var first = state.requestMode(ParsingMode.AGENT);
        var repeated = state.requestMode(ParsingMode.AGENT);

        assertEquals(first, repeated);
        assertTrue(repeated.hasPendingRequest());

        state.markApplied(state.selectForNextBatch());

        var applied = state.snapshot();
        var repeatedAfterApply = state.requestMode(ParsingMode.AGENT);

        assertEquals(applied, repeatedAfterApply);
        assertFalse(repeatedAfterApply.hasPendingRequest());
    }

    @Test
    void 배치_예약_전_요청이_왕복하면_마지막_요청만_선택한다() {
        var state = new AgentModeState(ParsingMode.CENTRAL);

        state.requestMode(ParsingMode.AGENT);
        state.requestMode(ParsingMode.CENTRAL);

        var beforeApply = state.snapshot();

        // 모드 값이 같아도 최신 요청 순번은 아직 수집기가 적용하지 않았다.
        assertEquals(ParsingMode.CENTRAL, beforeApply.requestedMode());
        assertEquals(ParsingMode.CENTRAL, beforeApply.appliedMode());
        assertEquals(2L, beforeApply.requestedRevision());
        assertEquals(0L, beforeApply.appliedRevision());
        assertTrue(beforeApply.hasPendingRequest());

        var selection = state.selectForNextBatch();

        assertEquals(ParsingMode.CENTRAL, selection.mode());
        assertEquals(2L, selection.revision());

        state.markApplied(selection);

        assertFalse(state.snapshot().hasPendingRequest());
        assertEquals(2L, state.snapshot().appliedRevision());
    }

    @Test
    void 이전_예약을_적용하면_최신_수신_ID와_실제_적용_ID가_구분된다() {
        var state = new AgentModeState(ParsingMode.CENTRAL);
        var controllerId = UUID.randomUUID();
        var first = new Command(new CommandId(controllerId, 1L), ParsingMode.AGENT);
        var second = new Command(new CommandId(controllerId, 2L), ParsingMode.AGENT);

        state.requestCommand(first);
        var reserved = state.selectForNextBatch();
        state.requestCommand(second);

        state.markApplied(reserved);

        var between = state.snapshot();
        assertEquals(second.id(), between.receivedCommandId());
        assertEquals(first.id(), between.appliedCommandId());
        assertEquals(ParsingMode.AGENT, between.appliedMode());
        assertTrue(between.hasPendingRequest());

        state.markApplied(state.selectForNextBatch());

        var completed = state.snapshot();
        assertEquals(second.id(), completed.appliedCommandId());
        assertFalse(completed.hasPendingRequest());
    }

    @Test
    void 현재와_같은_모드의_명령도_배치_적용을_기다리고_재수신은_멱등이다() {
        var state = new AgentModeState(ParsingMode.CENTRAL);
        var command = new Command(
                new CommandId(UUID.randomUUID(), 1L), ParsingMode.CENTRAL
        );

        var received = state.requestCommand(command);
        assertEquals(command.id(), received.receivedCommandId());
        assertNull(received.appliedCommandId());
        assertTrue(received.hasPendingRequest());
        assertEquals(received, state.requestCommand(command));

        var selection = state.selectForNextBatch();
        assertNull(state.snapshot().appliedCommandId());
        state.markApplied(selection);

        var applied = state.snapshot();
        assertEquals(command.id(), applied.appliedCommandId());
        assertFalse(applied.hasPendingRequest());
        assertEquals(applied, state.requestCommand(command));
    }

    @Test
    void 낮은_버전과_동일_버전의_다른_모드를_거부한다() {
        var state = new AgentModeState(ParsingMode.CENTRAL);
        var controllerId = UUID.randomUUID();
        var accepted = new Command(new CommandId(controllerId, 2L), ParsingMode.AGENT);
        var before = state.requestCommand(accepted);

        var older = new Command(new CommandId(controllerId, 1L), ParsingMode.AGENT);
        var conflict = new Command(accepted.id(), ParsingMode.CENTRAL);

        assertThrows(IllegalArgumentException.class, () -> state.requestCommand(older));
        assertThrows(IllegalArgumentException.class, () -> state.requestCommand(conflict));
        assertEquals(before, state.snapshot());
    }

    @Test
    void 응답의_유효_기간은_전송_시각부터_계산하고_경계에서_만료된다() {
        var clock = new AtomicLong(8_000_000L);
        var state = new AgentModeState(ParsingMode.CENTRAL, clock::get);
        var command = new Command(
                new CommandId(UUID.randomUUID(), 1L), ParsingMode.AGENT
        );

        // 요청 시작은 0ms, 응답 수신은 8ms, 유효 기간은 10ms다.
        state.acceptControl(command, 0L, 10L);
        assertEquals(command.id(), state.selectForNextBatch().commandId());

        clock.set(10_000_000L);
        var selection = state.selectForNextBatch();
        assertEquals(ParsingMode.CENTRAL, selection.mode());
        assertNull(selection.commandId());
        assertEquals(command.id(), state.snapshot().receivedCommandId());
        assertThrows(IllegalArgumentException.class,
                () -> state.acceptControl(command, 0L, 10L));
    }

    @Test
    void 제어가_끊겨도_예약은_적용하고_다음_배치는_그_적용_모드를_유지한다() {
        var clock = new AtomicLong();
        var state = new AgentModeState(ParsingMode.CENTRAL, clock::get);
        var controllerId = UUID.randomUUID();
        var first = new Command(new CommandId(controllerId, 1L), ParsingMode.AGENT);
        var second = new Command(new CommandId(controllerId, 2L), ParsingMode.CENTRAL);

        state.acceptControl(first, 0L, 20L);
        var reserved = state.selectForNextBatch();
        state.acceptControl(second, 0L, 20L);
        long secondRevision = state.snapshot().requestedRevision();

        state.suspendControl();
        state.markApplied(reserved);

        var fallback = state.selectForNextBatch();
        assertEquals(ParsingMode.AGENT, fallback.mode());
        assertEquals(first.id(), fallback.commandId());
        assertEquals(second.id(), state.snapshot().receivedCommandId());

        state.acceptControl(second, 0L, 20L);
        var recovered = state.selectForNextBatch();
        assertEquals(second.id(), recovered.commandId());
        assertEquals(secondRevision, recovered.revision());
    }
}