package com.seshat.demo;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.seshat.control.AgentControlService;
import com.seshat.control.AgentControlService.ControlException;
import com.seshat.control.AutoModeEvaluator;
import com.seshat.control.AutoModePolicy.Reason;
import com.seshat.control.dto.AgentControlMessages.*;
import com.seshat.ingestion.dto.ParsingMode;
import com.seshat.monitoring.AgentStatusService;
import com.seshat.queue.QueueStatsService;
import jakarta.annotation.PreDestroy;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.http.CacheControl;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

@RestController
@RequestMapping("/api/demo/scenario")
@ConditionalOnProperty(
        prefix = "seshat.demo", name = "enabled", havingValue = "true"
)
public class DemoScenarioController {

    private static final String SOURCE_ID = "demo-source";
    private static final int LINES_PER_MODE = 800;
    private static final int STRESS_LINES = 5_000;
    private static final int WRITE_BATCH_SIZE = 50;
    private static final long STRESS_NANOS = Duration.ofSeconds(10).toNanos();
    private static final Duration DRAIN_LIMIT = Duration.ofMinutes(3);
    private static final long LOG_SEED = 20261113L;
    private static final String LARGE_MESSAGE = "x".repeat(8192);
    private static final Duration WAIT_LIMIT = Duration.ofSeconds(30);
    private static final long RETENTION_NANOS = Duration.ofMinutes(5).toNanos();
    private static final int CLEANUP_THRESHOLD = 20;
    private static final System.Logger LOG =
            System.getLogger(DemoScenarioController.class.getName());

    private final AgentStatusService agents;
    private final AgentControlService control;
    private final AutoModeEvaluator autoMode;
    private final QueueStatsService stats;
    private final DemoRunCleanupService cleanup;
    private final int port;
    private final Path agentJar;
    private final ScheduledExecutorService executor = Executors.newScheduledThreadPool(2);
    private final ArrayDeque<FinishedRun> finishedRuns = new ArrayDeque<>();

    private volatile DemoRun current;

    public DemoScenarioController(
            AgentStatusService agents,
            AgentControlService control,
            Optional<AutoModeEvaluator> autoMode,
            QueueStatsService stats,
            DemoRunCleanupService cleanup,
            @Value("${server.port:8080}") int port,
            @Value("${seshat.demo.agent-jar:build/libs/agent.jar}") String agentJar
    ) {
        this.agents = agents;
        this.control = control;
        this.autoMode = autoMode.filter(AutoModeEvaluator::demoEnabled).orElse(null);
        this.stats = stats;
        this.cleanup = cleanup;
        this.port = port;
        this.agentJar = Path.of(agentJar).toAbsolutePath().normalize();
        executor.scheduleWithFixedDelay(this::pruneFinishedRuns, 1, 1, TimeUnit.SECONDS);
    }

    public enum Scenario {
        CENTRAL, AGENT, SWITCH, STRESS, AUTO_SWITCH
    }

    public enum Phase {
        STARTING, WRITING, WAITING_FOR_SWITCH, DRAINING, COMPLETED, FAILED
    }

    public enum SwitchPhase {
        SENT, RECEIVED, APPLIED
    }

    public enum AutoStage {
        BASELINE, CENTRAL_BUSY, AGENT_BUSY
    }

    public record AutoProgress(
            AutoStage stage,
            @JsonProperty("central_cpu") int centralCpu,
            @JsonProperty("agent_cpu") int agentCpu,
            @JsonProperty("applied_mode") ParsingMode appliedMode
    ) {}

    public record StartRequest(Scenario scenario) {}
    public record CleanupRequest(@JsonProperty("run_ids") List<String> runIds) {}

    public record DemoRun(
            @JsonProperty("run_id") String runId,
            Scenario scenario,
            Phase phase,
            String message,
            @JsonProperty("switch_phase") SwitchPhase switchPhase,
            @JsonProperty("auto_progress") AutoProgress autoProgress,
            @JsonProperty("expected_count") int expectedCount,
            @JsonProperty("generated_count") int generatedCount,
            @JsonProperty("generated_per_second") double generatedPerSecond
    ) {
        boolean running() {
            return phase != Phase.COMPLETED && phase != Phase.FAILED;
        }
    }

    public record DemoStatus(DemoRun run, @JsonProperty("auto_enabled") boolean autoEnabled) {}

    private record FinishedRun(String runId, long startedAtNanos) {}

    @GetMapping
    public DemoStatus status() {
        return new DemoStatus(current, autoMode != null);
    }

    @GetMapping("/parse-failures")
    public long parseFailures(@RequestParam("run_id") String runId) {
        return stats.countParseFailures(runId);
    }

    @GetMapping("/history")
    public ResponseEntity<List<DemoRunCleanupService.RunEntry>> history() {
        return ResponseEntity.ok().cacheControl(CacheControl.noStore()).body(cleanup.history());
    }

    @GetMapping("/cleanup")
    public ResponseEntity<List<DemoRunCleanupService.PreviewEntry>> preview(
            @RequestParam(value = "run_id", required = false) String runId
    ) throws IOException {
        return ResponseEntity.ok().cacheControl(CacheControl.noStore()).body(cleanup.preview(runId));
    }

    @PostMapping(value = "/cleanup", consumes = MediaType.APPLICATION_JSON_VALUE)
    public List<DemoRunCleanupService.CleanupResult> delete(@RequestBody CleanupRequest request) {
        var results = cleanup.delete(request.runIds());
        for (var result : results) {
            if (result.deleted()) clearCurrent(result.runId());
        }
        return results;
    }

    private synchronized void clearCurrent(String runId) {
        if (current != null && current.runId().equals(runId)) current = null;
    }

    @PostMapping
    @ResponseStatus(HttpStatus.ACCEPTED)
    public synchronized DemoRun start(@RequestBody StartRequest request) {
        if (request.scenario() == null) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "시나리오를 선택하세요.");
        }
        if (current != null && current.running()) {
            throw new ResponseStatusException(HttpStatus.CONFLICT, "다른 시연이 실행 중입니다.");
        }
        if (request.scenario() == Scenario.AUTO_SWITCH && autoMode == null) {
            throw new ResponseStatusException(HttpStatus.CONFLICT, "자동 전환 시연이 꺼져 있습니다.");
        }
        if (!Files.isRegularFile(agentJar)) {
            throw new ResponseStatusException(
                    HttpStatus.SERVICE_UNAVAILABLE, "agent.jar를 먼저 빌드하세요."
            );
        }

        String runId = "demo-" + request.scenario().name().toLowerCase(Locale.ROOT)
                + "-" + UUID.randomUUID();
        var run = new DemoRun(
                runId, request.scenario(), Phase.STARTING, "시연 에이전트를 시작합니다.", null, null,
                switch (request.scenario()) {
                    case STRESS -> STRESS_LINES;
                    case SWITCH -> LINES_PER_MODE * 2;
                    case AUTO_SWITCH -> LINES_PER_MODE * 3;
                    default -> LINES_PER_MODE;
                }, 0, 0
        );
        long startedAtNanos = System.nanoTime();
        cleanup.register(runId, SOURCE_ID, request.scenario());
        current = run;
        executor.execute(() -> execute(run, startedAtNanos));
        return run;
    }

    private void execute(DemoRun run, long startedAtNanos) {
        Process agent = null;
        boolean interrupted = false;
        String failure = null;
        boolean stress = run.scenario() == Scenario.STRESS;
        Random random = new Random(LOG_SEED);
        Path directory = DemoRunCleanupService.workDirectory(run.runId());

        try {
            Files.createDirectories(directory.getParent());
            Files.createDirectory(directory);
            Path input = Files.createFile(directory.resolve("app.log"));

            ParsingMode initialMode = run.scenario() == Scenario.AGENT
                    ? ParsingMode.AGENT : ParsingMode.CENTRAL;
            agent = launchAgent(run.runId(), initialMode, input, directory);

            Process process = agent;
            await(process, () -> targetFor(run.runId()) != null,
                    "에이전트가 상태를 보고하지 않았습니다.");
            Target target = targetFor(run.runId());

            if (run.scenario() == Scenario.AUTO_SWITCH) {
                try {
                    demonstrateAutomaticSwitch(run, input, process, random, target);
                } finally {
                    autoMode.clearDemoCpu(target);
                }
                return;
            }

            update(run, Phase.WRITING, stress ? "혼합 로그를 집중적으로 보내는 중입니다."
                    : modeLabel(initialMode) + " 로그를 보내는 중입니다.");
            long writingNanos = writeLines(run, input, process, random, 1,
                    stress ? STRESS_LINES : LINES_PER_MODE, "before", 0, stress);

            if (run.scenario() == Scenario.SWITCH) {
                awaitStored(process, run.runId(), LINES_PER_MODE);
                await(process, () -> controlActive(target),
                        "에이전트 제어 세션을 찾을 수 없습니다.");

                var command = control.issue(
                        new CreateCommandRequest(target, UUID.randomUUID(), ParsingMode.AGENT)
                );
                update(run, Phase.WAITING_FOR_SWITCH,
                        "에이전트 파싱 명령을 보냈습니다. 다음 배치에서 적용됩니다.", SwitchPhase.SENT);

                await(process, () -> {
                    var status = control.status(target);
                    return command.equals(status.command())
                            && status.phase() == CommandPhase.PENDING_APPLY;
                }, "에이전트가 전환 명령을 받지 못했습니다.");

                update(run, Phase.WAITING_FOR_SWITCH,
                        "에이전트가 명령을 받았습니다. 다음 배치에서 적용합니다.", SwitchPhase.RECEIVED);

                // 대시보드에서 적용 대기를 볼 시간을 준 뒤 새 배치를 만든다.
                Thread.sleep(5500);
                update(run, Phase.WRITING, "전환 후 로그를 보내는 중입니다.");
                writeLines(run, input, process, random, LINES_PER_MODE + 1, LINES_PER_MODE,
                        "after", writingNanos, false);

                await(process, () -> {
                    var status = control.status(target);
                    return command.equals(status.command())
                            && status.phase() == CommandPhase.APPLIED
                            && status.appliedMode() == ParsingMode.AGENT;
                }, "에이전트가 전환 적용을 보고하지 않았습니다.");
                update(run, Phase.DRAINING, "에이전트 파싱 전환을 확인했습니다.", SwitchPhase.APPLIED);
            } else {
                update(run, Phase.DRAINING, "중앙의 저장 완료를 기다리는 중입니다.");
            }
            awaitStored(process, run.runId(), current.generatedCount());
        } catch (InterruptedException error) {
            interrupted = true;
            failure = "시연이 중단됐습니다.";
        } catch (IOException | RuntimeException error) {
            failure = error.getMessage() == null
                    ? "시연 실행에 실패했습니다." : error.getMessage();
            LOG.log(System.Logger.Level.WARNING, "시연 실패 " + run.runId(), error);
        } finally {
            try {
                if (agent != null && agent.isAlive()) {
                    agent.destroy();
                    if (!agent.waitFor(3, TimeUnit.SECONDS)) {
                        agent.destroyForcibly();
                        if (!agent.waitFor(3, TimeUnit.SECONDS)) {
                            failure = "시연 에이전트를 종료하지 못했습니다.";
                        }
                    }
                }
            } catch (InterruptedException error) {
                agent.destroyForcibly();
                interrupted = true;
                failure = "시연이 중단됐습니다.";
            } finally {
                String message = failure;
                if (agent != null && agent.isAlive()) {
                    // 종료 확인 뒤에만 만료 대상으로 등록해 늦은 상태 보고와 겹치지 않게 한다.
                    agent.onExit().thenRun(() -> finish(
                            run, startedAtNanos, message
                    ));
                } else {
                    finish(run, startedAtNanos, message);
                }
                if (interrupted) {
                    Thread.currentThread().interrupt();
                }
            }
        }
    }

    private void demonstrateAutomaticSwitch(DemoRun run, Path input, Process process, Random random, Target target)
            throws IOException, InterruptedException {
        setAutoStage(run, target, new AutoProgress(AutoStage.BASELINE, 60, 60, ParsingMode.CENTRAL),
                Phase.WRITING, "중앙 파싱으로 로그를 보내는 중입니다.");
        long writingNanos = writeLines(run, input, process, random, 1, LINES_PER_MODE, "baseline", 0, false);
        awaitStored(process, run.runId(), LINES_PER_MODE);
        await(process, () -> controlActive(target), "에이전트 제어 세션을 찾을 수 없습니다.");

        writingNanos += demonstrateAutomaticStage(run, input, process, random, target, LINES_PER_MODE + 1,
                new AutoProgress(AutoStage.CENTRAL_BUSY, 90, 20, ParsingMode.CENTRAL),
                ParsingMode.AGENT, Reason.CENTRAL_BUSY_AGENT_AVAILABLE, "offloaded", writingNanos);

        demonstrateAutomaticStage(run, input, process, random, target, 2 * LINES_PER_MODE + 1,
                new AutoProgress(AutoStage.AGENT_BUSY, 20, 90, ParsingMode.AGENT),
                ParsingMode.CENTRAL, Reason.AGENT_BUSY_CENTRAL_AVAILABLE, "returned", writingNanos);
    }

    private long demonstrateAutomaticStage(DemoRun run, Path input, Process process, Random random, Target target,
                                           int first, AutoProgress progress, ParsingMode next, Reason reason,
                                           String segment, long priorNanos)
            throws IOException, InterruptedException {
        setAutoStage(run, target, progress, Phase.WAITING_FOR_SWITCH,
                modeLabel(next) + " 자동 전환 조건을 평가하는 중입니다.");
        await(process, () -> {
            var status = control.status(target);
            return reason.name().equals(status.commandReason()) && status.command().mode() == next;
        }, modeLabel(next) + " 자동 명령이 발행되지 않았습니다.");

        var command = control.status(target).command();
        update(run, Phase.WAITING_FOR_SWITCH, modeLabel(next) + " 자동 명령을 발행했습니다.", SwitchPhase.SENT);
        await(process, () -> {
            var status = control.status(target);
            return command.equals(status.command()) && status.phase() == CommandPhase.PENDING_APPLY;
        }, "에이전트가 자동 전환 명령을 수신하지 못했습니다.");

        update(run, Phase.WAITING_FOR_SWITCH, "명령을 받았습니다. 다음 배치에서 적용합니다.", SwitchPhase.RECEIVED);
        // 대시보드에서 적용 대기를 볼 수 있도록 CPU 조건과 로그 경계를 유지한다.
        Thread.sleep(2500);
        update(run, Phase.WRITING, "새 배치를 보내는 중입니다.");
        long writingNanos = writeLines(run, input, process, random, first, LINES_PER_MODE, segment, priorNanos, false);

        await(process, () -> {
            var status = control.status(target);
            return command.equals(status.command()) && status.phase() == CommandPhase.APPLIED
                    && status.appliedMode() == next;
        }, "에이전트가 자동 전환 적용을 보고하지 않았습니다.");

        update(run, Phase.DRAINING, modeLabel(next) + " 적용을 확인했습니다.", SwitchPhase.APPLIED,
                new AutoProgress(progress.stage(), progress.centralCpu(), progress.agentCpu(), next));
        awaitStored(process, run.runId(), first + LINES_PER_MODE - 1);
        return writingNanos;
    }

    private void setAutoStage(DemoRun run, Target target, AutoProgress progress, Phase phase, String message) {
        autoMode.setDemoCpu(target, progress.centralCpu() / 100.0, progress.agentCpu() / 100.0);
        update(run, phase, message, null, progress);
    }

    private Process launchAgent(
            String runId, ParsingMode mode, Path input, Path directory
    ) throws IOException {
        String executable = System.getProperty("os.name").startsWith("Windows")
                ? "java.exe" : "java";
        String java = Path.of(System.getProperty("java.home"), "bin", executable).toString();
        String endpoint = "http://127.0.0.1:" + port + "/api/logs/batches";

        return new ProcessBuilder(
                java, "-jar", agentJar.toString(),
                runId, SOURCE_ID, mode.name(), input.toString(), endpoint,
                "16", directory.resolve("queue").toString()
        )
                .redirectErrorStream(true)
                .redirectOutput(directory.resolve("agent.log").toFile())
                .start();
    }

    private Target targetFor(String runId) {
        return agents.list(runId).agents().stream()
                .filter(agent -> "ONLINE".equals(agent.status()))
                .map(agent -> agent.heartbeat())
                .filter(heartbeat -> SOURCE_ID.equals(heartbeat.sourceId()))
                .map(heartbeat -> new Target(runId, SOURCE_ID, heartbeat.instanceId()))
                .findFirst()
                .orElse(null);
    }

    private boolean controlActive(Target target) {
        try {
            return control.status(target).active();
        } catch (ControlException error) {
            if (error.status() == HttpStatus.NOT_FOUND) {
                return false;
            }
            throw error;
        }
    }

    private void awaitStored(Process process, String runId, int expected)
            throws IOException, InterruptedException {
        await(process, () -> {
            var result = stats.getStats(runId);
            if (result.storageFailedCount() > 0 || result.totalCount() > expected) {
                throw new IllegalStateException("시연 로그의 저장 실패 또는 초과 수신을 확인했습니다.");
            }
            return result.totalCount() == expected && result.completedCount() == expected;
        }, "저장 완료 대기 시간이 초과됐습니다.", DRAIN_LIMIT);
    }

    private void await(Process process, BooleanSupplier ready, String timeoutMessage)
            throws IOException, InterruptedException {
        await(process, ready, timeoutMessage, WAIT_LIMIT);
    }

    private void await(Process process, BooleanSupplier ready, String timeoutMessage, Duration limit)
            throws IOException, InterruptedException {
        long start = System.nanoTime();
        while (System.nanoTime() - start < limit.toNanos()) {
            if (!process.isAlive()) {
                throw new IOException("시연 에이전트가 종료됐습니다. 시연 폴더의 agent.log를 확인하세요.");
            }
            if (ready.getAsBoolean()) {
                return;
            }
            Thread.sleep(250);
        }
        throw new IOException(timeoutMessage);
    }

    private long writeLines(DemoRun run, Path input, Process agent, Random random, int first,
                            int count, String segment, long priorNanos, boolean stress)
            throws IOException, InterruptedException {
        long started = System.nanoTime();
        int written = 0;
        while (written < count && (!stress || System.nanoTime() - started < STRESS_NANOS)) {
            if (!agent.isAlive()) {
                throw new IOException("로그를 생성하는 동안 시연 에이전트가 종료됐습니다.");
            }

            int batchSize = Math.min(WRITE_BATCH_SIZE, count - written);
            var batch = new StringBuilder();
            for (int index = 0; index < batchSize; index++) {
                appendSample(batch, random, first + written + index, segment);
            }
            Files.writeString(input, batch, StandardOpenOption.APPEND);
            written += batchSize;

            int generated = first + written - 1;
            double seconds = (priorNanos + System.nanoTime() - started) / 1_000_000_000.0;
            var status = current;
            current = new DemoRun(run.runId(), run.scenario(), status.phase(), status.message(),
                    status.switchPhase(), status.autoProgress(), run.expectedCount(), generated, generated / seconds);

            if (written < count) {
                Thread.sleep(stress ? 100 : 250);
            }
        }
        return System.nanoTime() - started;
    }

    private static void appendSample(StringBuilder batch, Random random, int sequence, String segment) {
        int sample = random.nextInt(10);
        String kind = sample == 0 ? "invalid" : sample < 3 ? "large" : "normal";
        batch.append("{\"sequence\":").append(sequence)
                .append(",\"segment\":\"").append(segment)
                .append("\",\"case\":\"").append(kind).append("\",\"message\":\"")
                .append(sample < 3 && sample != 0 ? LARGE_MESSAGE : "Seshat demo")
                .append('"');
        if (sample != 0) {
            batch.append('}');
        }
        batch.append('\n');
    }

    private static String modeLabel(ParsingMode mode) {
        return mode == ParsingMode.CENTRAL ? "중앙 파싱" : "에이전트 파싱";
    }

    private void update(DemoRun run, Phase phase, String message) {
        update(run, phase, message, current.switchPhase());
    }

    private void update(DemoRun run, Phase phase, String message, SwitchPhase switchPhase) {
        update(run, phase, message, switchPhase, current.autoProgress());
    }

    private void update(DemoRun run, Phase phase, String message, SwitchPhase switchPhase, AutoProgress autoProgress) {
        var previous = current;
        current = new DemoRun(run.runId(), run.scenario(), phase, message, switchPhase,
                autoProgress, run.expectedCount(), previous.generatedCount(), previous.generatedPerSecond());
    }

    private synchronized void finish(
            DemoRun run, long startedAtNanos, String failure
    ) {
        cleanup.finish(run.runId(), failure == null);
        if (failure == null) {
            update(run, Phase.COMPLETED,
                    "로그 " + current.generatedCount() + "건 저장을 확인했습니다.");
        } else {
            update(run, Phase.FAILED, failure);
        }

        finishedRuns.addLast(new FinishedRun(run.runId(), startedAtNanos));
        pruneFinishedRuns();
    }

    private synchronized void pruneFinishedRuns() {
        long now = System.nanoTime();
        while (!finishedRuns.isEmpty()) {
            FinishedRun oldest = finishedRuns.getFirst();
            if (finishedRuns.size() < CLEANUP_THRESHOLD
                    && now - oldest.startedAtNanos() < RETENTION_NANOS) {
                return;
            }

            agents.removeRun(oldest.runId());
            control.removeRun(oldest.runId());
            finishedRuns.removeFirst();
            if (current != null && current.runId().equals(oldest.runId())) {
                current = null;
            }
        }
    }

    @PreDestroy
    void stop() {
        executor.shutdownNow();
    }
}
