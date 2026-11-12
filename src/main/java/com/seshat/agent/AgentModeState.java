package com.seshat.agent;

import com.seshat.control.dto.AgentControlMessages.Command;
import com.seshat.control.dto.AgentControlMessages.CommandId;
import com.seshat.ingestion.dto.ParsingMode;

import java.util.Objects;
import java.util.function.LongSupplier;

/**
 * 단일 수집기의 요청과 적용 상태를 관리한다.
 * 로컬 순번은 프로세스 내부에서 배치 예약을 구분하는 데 사용한다.
 */
public final class AgentModeState {

    private BatchSelection requested;
    private BatchSelection applied;
    private Command lastReceivedCommand;

    private final LongSupplier nanoTime;

    // HTTP 제어가 시작된 뒤에는 유효한 응답이 있어야 중앙 명령을 예약한다.
    private boolean controlManaged;
    private ControlLease controlLease;

    public AgentModeState(ParsingMode initialMode) {
        this(initialMode, System::nanoTime);
    }

    AgentModeState(ParsingMode initialMode, LongSupplier nanoTime) {
        this.nanoTime = Objects.requireNonNull(nanoTime);
        requested = new BatchSelection(Objects.requireNonNull(initialMode), 0L, null);
        applied = requested;
    }

    synchronized void acceptControl(
            Command command,
            long requestStartedNanos,
            long validForMs
    ) {
        if (validForMs <= 0 || validForMs > Long.MAX_VALUE / 1_000_000L) {
            throw new IllegalArgumentException("제어 응답의 유효 기간이 범위를 벗어났습니다.");
        }

        var lease = new ControlLease(requestStartedNanos, validForMs * 1_000_000L);
        if (!lease.isValidAt(nanoTime.getAsLong())) {
            throw new IllegalArgumentException("제어 응답이 도착하기 전에 만료됐습니다.");
        }

        if (command == null) {
            suspendControl();
            return;
        }

        requestCommand(command);
        controlManaged = true;
        controlLease = lease;
    }

    synchronized void suspendControl() {
        controlManaged = true;
        controlLease = null;
    }

    /**
     * 새 배치에 사용할 모드와 명령 ID를 함께 예약한다.
     */
    synchronized BatchSelection selectForNextBatch() {
        if (requested.commandId() != null && controlManaged
                && (controlLease == null || !controlLease.isValidAt(nanoTime.getAsLong()))) {
            return applied;
        }
        return requested;
    }

    private record ControlLease(long startedAtNanos, long durationNanos) {
        private boolean isValidAt(long now) {
            long elapsed = now - startedAtNanos;
            return elapsed >= 0L && elapsed < durationNanos;
        }
    }

    /**
     * 다음 새 배치에 사용할 로컬 모드를 요청한다.
     * 현재 요청과 모드가 같으면 순번을 유지한다.
     */
    public synchronized Snapshot requestMode(ParsingMode mode) {
        Objects.requireNonNull(mode);

        if (requested.mode() != mode) {
            requested = new BatchSelection(
                    mode, Math.incrementExact(requested.revision()), null
            );
        }
        return snapshot();
    }

    /**
     * 제어 클라이언트가 대상과 중앙 인스턴스를 확인한 명령을 수신한다.
     * 새 명령 ID는 모드가 같아도 별도의 배치 적용을 기다린다.
     */
    public synchronized Snapshot requestCommand(Command command) {
        Objects.requireNonNull(command);

        CommandId id = command.id();
        if (lastReceivedCommand != null
                && lastReceivedCommand.id().controllerInstanceId()
                .equals(id.controllerInstanceId())) {
            long lastVersion = lastReceivedCommand.id().commandVersion();

            if (id.commandVersion() < lastVersion) {
                throw new IllegalArgumentException("이미 수신한 버전보다 낮은 명령입니다.");
            }
            if (id.commandVersion() == lastVersion) {
                if (!lastReceivedCommand.equals(command)) {
                    throw new IllegalArgumentException("같은 버전의 명령 내용이 다릅니다.");
                }
                return snapshot();
            }
        }

        requested = new BatchSelection(
                command.mode(), Math.incrementExact(requested.revision()), id
        );
        lastReceivedCommand = command;
        return snapshot();
    }

    public synchronized Snapshot snapshot() {
        return new Snapshot(
                requested.mode(),
                requested.revision(),
                applied.mode(),
                applied.revision(),
                lastReceivedCommand == null ? null : lastReceivedCommand.id(),
                applied.commandId()
        );
    }

    /**
     * 첫 이벤트를 수용한 배치의 예약 상태를 적용 상태로 기록한다.
     */
    synchronized void markApplied(BatchSelection selection) {
        applied = Objects.requireNonNull(selection);
    }

    record BatchSelection(ParsingMode mode, long revision, CommandId commandId) {
    }

    public record Snapshot(
            ParsingMode requestedMode,
            long requestedRevision,
            ParsingMode appliedMode,
            long appliedRevision,
            CommandId receivedCommandId,
            CommandId appliedCommandId
    ) {
        public boolean hasPendingRequest() {
            return requestedRevision != appliedRevision;
        }
    }
}