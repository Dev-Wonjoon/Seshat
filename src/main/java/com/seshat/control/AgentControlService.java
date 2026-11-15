package com.seshat.control;

import com.seshat.control.dto.AgentControlMessages.Command;
import com.seshat.control.dto.AgentControlMessages.CommandId;
import com.seshat.control.dto.AgentControlMessages.CommandPhase;
import com.seshat.control.dto.AgentControlMessages.CreateCommandRequest;
import com.seshat.control.dto.AgentControlMessages.StatusResponse;
import com.seshat.control.dto.AgentControlMessages.SyncRequest;
import com.seshat.control.dto.AgentControlMessages.SyncResponse;
import com.seshat.control.dto.AgentControlMessages.Target;
import com.seshat.ingestion.dto.ParsingMode;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.*;
import java.util.function.LongSupplier;

@Service
public class AgentControlService {

    private final UUID controllerId = UUID.randomUUID();
    private final long leaseMs;
    private final long leaseNanos;
    private final LongSupplier nanoTime;
    private final AutoModeEvaluator autoMode;

    private final Map<AgentKey, Session> sessions = new HashMap<>();
    private long appliedSinceNanos;
    private AutoModePolicy.Reason automaticReason;
    private final Map<Target, NavigableMap<Long, Command>> commandHistory =
            new HashMap<>();
    private final Map<UUID, IssuedRequest> issuedRequests = new HashMap<>();

    @Autowired
    public AgentControlService(
            @Value("${seshat.control.lease-ms:20000}") long leaseMs,
            Optional<AutoModeEvaluator> autoMode
    ) {
        this(leaseMs, System::nanoTime, autoMode.orElse(null));
    }

    AgentControlService(long leaseMs, LongSupplier nanoTime) {
        this(leaseMs, nanoTime, null);
    }

    AgentControlService(long leaseMs, LongSupplier nanoTime, AutoModeEvaluator autoMode) {
        if (leaseMs <= 0) {
            throw new IllegalArgumentException("제어 세션 유효 기간은 양수여야 합니다.");
        }

        this.leaseMs = leaseMs;
        this.leaseNanos = Math.multiplyExact(leaseMs, 1_000_000L);
        this.nanoTime = Objects.requireNonNull(nanoTime);
        this.autoMode = autoMode;
    }

    public synchronized SyncResponse sync(SyncRequest request) {
        Target target = request.target();
        var key = new AgentKey(target.runId(), target.sourceId());
        long now = nanoTime.getAsLong();
        Session session = sessions.get(key);

        if (session == null || !session.target.equals(target)) {
            if (session != null && isActive(session, now)) {
                throw new ControlException(
                        HttpStatus.CONFLICT,
                        "CONTROL_SESSION_CONFLICT",
                        "같은 실행과 소스에 유효한 제어 세션이 있습니다."
                );
            }

            session = new Session(target);
        }

        Command received = reportedCommand(target, request.receivedCommandId());
        Command applied = reportedCommand(target, request.appliedCommandId());

        if (applied != null && applied.mode() != request.appliedMode()) {
            throw new ControlException(
                    HttpStatus.BAD_REQUEST,
                    "INVALID_APPLIED_MODE",
                    "보고한 적용 모드가 발행한 명령의 모드와 다릅니다."
            );
        }

        session.observe(received, applied, request.appliedMode(), now);
        session.lastSyncNanos = now;
        sessions.put(key, session);
        return controlResponse(session, now);
    }

    public synchronized Command issue(CreateCommandRequest request) {
        IssuedRequest prior = issuedRequests.get(request.requestId());

        // 응답이 유실됐을 때 같은 요청으로 최초 발행 결과를 받을 수 있다.
        if (prior != null) {
            if (!prior.request().equals(request)) {
                throw new ControlException(
                        HttpStatus.CONFLICT,
                        "REQUEST_ID_CONFLICT",
                        "이미 사용한 request_id의 요청 내용이 다릅니다."
                );
            }
            return prior.command();
        }

        Session session = requireSession(request.target());
        if (!isActive(session, nanoTime.getAsLong())) {
            throw new ControlException(
                    HttpStatus.CONFLICT,
                    "CONTROL_SESSION_EXPIRED",
                    "대상 제어 세션이 만료됐습니다."
            );
        }

        Command command = publish(session, request.mode(), null);
        issuedRequests.put(request.requestId(), new IssuedRequest(request, command));
        return command;
    }

    private Command publish(
            Session session,
            ParsingMode mode,
            AutoModePolicy.Reason automaticReason
    ) {
        var history = commandHistory.computeIfAbsent(
                session.target,  target -> new TreeMap<>()
        );
        long lastVersion = history.isEmpty() ? 0L : history.lastKey();
        long version = Math.incrementExact(lastVersion);
        var command = new Command(new CommandId(controllerId, version), mode);

        history.put(version, command);
        session.command = command;
        session.phase = CommandPhase.PENDING_DELIVERY;
        session.automaticReason = automaticReason;
        return command;
    }

    private SyncResponse controlResponse(Session session, long now) {
        boolean pending = session.phase == CommandPhase.PENDING_DELIVERY
                || session.phase == CommandPhase.PENDING_APPLY;

        if (autoMode == null || (pending && session.automaticReason == null)) {
            return new SyncResponse(session.target, controllerId, leaseMs, session.command);
        }

        Duration heldFor = Duration.ofNanos(now - session.appliedSinceNanos);
        var evaluation = autoMode.evaluate(session.target, session.appliedMode, heldFor);
        var decision = evaluation.decision();
        ParsingMode next = decision.nextMode();

        if (next == session.appliedMode) {
            // 보류된 자동 명령의 ID와 상태는 이후 수신·적용 보고에 사용한다.
            Command manualCommand = session.automaticReason == null ? session.command : null;
            return new SyncResponse(session.target, controllerId, leaseMs, manualCommand);
        }

        if (pending && session.command.mode() != next) {
            return new SyncResponse(session.target, controllerId, leaseMs, null);
        }

        Command command = pending ? session.command : publish(session, next, decision.reason());
        long validForMs = Math.min(leaseMs, evaluation.validForMs());
        return new SyncResponse(session.target, controllerId, validForMs, command);
    }

    public synchronized StatusResponse status(Target target) {
        Session session = requireSession(target);
        return new StatusResponse(
                target,
                controllerId,
                isActive(session, nanoTime.getAsLong()),
                session.appliedMode,
                session.command,
                session.phase,
                session.automaticReason == null ? null : session.automaticReason.name()
        );
    }

    public synchronized void removeRun(String runId) {
        sessions.keySet().removeIf(key -> key.runId().equals(runId));
        commandHistory.keySet().removeIf(target -> target.runId().equals(runId));
        issuedRequests.values().removeIf(
                issued -> issued.request().target().runId().equals(runId)
        );
    }

    private Session requireSession(Target target) {
        if (target.runId().isBlank() || target.sourceId().isBlank()) {
            throw new ControlException(
                    HttpStatus.BAD_REQUEST,
                    "INVALID_REQUEST",
                    "run_id와 source_id는 비어 있을 수 없습니다."
            );
        }

        Session session = sessions.get(new AgentKey(target.runId(), target.sourceId()));
        if (session == null || !session.target.equals(target)) {
            throw new ControlException(
                    HttpStatus.NOT_FOUND,
                    "CONTROL_TARGET_NOT_FOUND",
                    "등록된 제어 대상을 찾을 수 없습니다."
            );
        }
        return session;
    }

    private Command reportedCommand(Target target, CommandId id) {
        if (id == null || !controllerId.equals(id.controllerInstanceId())) {
            return null;
        }

        var history = commandHistory.get(target);
        Command command = history == null ? null : history.get(id.commandVersion());

        if (command == null) {
            throw new ControlException(
                    HttpStatus.BAD_REQUEST,
                    "UNKNOWN_COMMAND_ID",
                    "해당 대상에 발행한 명령 ID를 찾을 수 없습니다."
            );
        }
        return command;
    }

    private boolean isActive(Session session, long now) {
        return now - session.lastSyncNanos < leaseNanos;
    }

    private record AgentKey(String runId, String sourceId) {
    }

    private record IssuedRequest(CreateCommandRequest request, Command command) {
    }

    private static final class Session {
        private final Target target;
        private long lastSyncNanos;
        private long lastAppliedVersion;
        private ParsingMode appliedMode;
        private Command command;
        private CommandPhase phase;
        private long appliedSinceNanos;
        private AutoModePolicy.Reason automaticReason;

        private Session(Target target) {
            this.target = target;
        }

        private void observe(
                Command received, Command applied, ParsingMode mode, long now
        ) {
            long appliedVersion = applied == null ? 0L : applied.id().commandVersion();

            // 이전 배치의 늦은 보고가 현재 적용 상태를 되돌리지 않게 한다.
            if (appliedVersion >= lastAppliedVersion) {
                if (appliedMode != mode || appliedVersion > lastAppliedVersion) {
                    appliedSinceNanos = now;
                }
                lastAppliedVersion = appliedVersion;
                appliedMode = mode;
            }

            if (command != null && command.equals(applied)) {
                phase = CommandPhase.APPLIED;
            } else if (phase == CommandPhase.PENDING_DELIVERY && command.equals(received)) {
                phase = CommandPhase.PENDING_APPLY;
            }
        }
    }

    public static final class ControlException extends RuntimeException {
        private final HttpStatus status;
        private final String code;

        private ControlException(HttpStatus status, String code, String message) {
            super(message);
            this.status = status;
            this.code = code;
        }

        public HttpStatus status() {
            return status;
        }

        public String code() {
            return code;
        }
    }
}