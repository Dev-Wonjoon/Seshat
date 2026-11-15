package com.seshat.control;

import com.seshat.agent.AgentMain;
import com.seshat.control.dto.AgentControlMessages.Command;
import com.seshat.control.dto.AgentControlMessages.CommandPhase;
import com.seshat.control.dto.AgentControlMessages.StatusResponse;
import com.seshat.control.dto.AgentControlMessages.Target;
import com.seshat.ingestion.dto.ParsingMode;
import com.seshat.monitoring.AgentStatusService;
import com.seshat.monitoring.AgentStatusService.ControlCpuSample;
import com.seshat.monitoring.SystemStatusService;
import com.seshat.monitoring.dto.SystemStatusResponse;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.core.env.Environment;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;
import tools.jackson.databind.json.JsonMapper;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicReference;

import static com.seshat.control.dto.AgentControlMessages.CommandPhase.APPLIED;
import static com.seshat.control.dto.AgentControlMessages.CommandPhase.PENDING_APPLY;
import static com.seshat.ingestion.dto.ParsingMode.AGENT;
import static com.seshat.ingestion.dto.ParsingMode.CENTRAL;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.when;

@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = {
                "server.address=127.0.0.1",
                "spring.datasource.url=jdbc:sqlite::memory:",
                "spring.datasource.hikari.maximum-pool-size=1",
                "spring.datasource.hikari.minimum-idle=1",
                "seshat.queue.worker-enabled=false",
                "seshat.control.auto.enabled=true",
                "seshat.control.auto.central-low=0.40",
                "seshat.control.auto.central-high=0.80",
                "seshat.control.auto.agent-low=0.45",
                "seshat.control.auto.agent-high=0.85",
                "seshat.control.auto.minimum-hold-ms=500"
        }
)
class AutoModeHttpIntegrationTest {

    @TempDir
    Path directory;

    @Autowired
    private Environment environment;

    @Autowired
    private JdbcTemplate jdbc;

    @Autowired
    private JsonMapper mapper;

    @MockitoBean
    private SystemStatusService systems;

    @MockitoSpyBean
    private AgentStatusService agents;

    private final String runId = "auto-http-" + UUID.randomUUID();
    private final UUID centralMetricsId = UUID.randomUUID();
    private final AtomicReference<Loads> loads = new AtomicReference<>(new Loads(0.60, 0.60));
    private final AtomicReference<Throwable> agentFailure = new AtomicReference<>();
    private Thread agent;

    @AfterEach
    void stopAgent() throws InterruptedException {
        if (agent != null) {
            agent.interrupt();
            assertTrue(agent.join(Duration.ofSeconds(45)), "에이전트 종료 시간 초과");
        }
    }

    @Test
    void 양방향_자동_명령을_새_배치에_적용하고_SQLite에_보관한다() throws Exception {
        when(systems.snapshot()).thenAnswer(invocation -> centralSample());
        doAnswer(invocation -> {
            var actual = (ControlCpuSample) invocation.callRealMethod();
            // 실제 하트비트의 대상 일치와 유효 기간을 사용한다.
            return new ControlCpuSample(loads.get().agent(), actual.validForMs());
        }).when(agents).controlCpuSample(anyString(), anyString(), any(UUID.class));

        String base = "http://127.0.0.1:" + environment.getRequiredProperty("local.server.port");
        Path source = Files.createFile(directory.resolve("input.ndjson"));
        String baseline = "{\"message\":\"baseline\"}";
        String offloaded = "{\"message\":\"offloaded\"}";
        String returned = "{\"message\":\"returned\"}";
        append(source, baseline);
        startAgent(base, source);

        try (var client = HttpClient.newHttpClient()) {
            Target target = await("에이전트 하트비트", () -> agents.list(runId).agents().stream()
                    .map(item -> item.heartbeat())
                    .map(heartbeat -> new Target(
                            heartbeat.runId(), heartbeat.sourceId(), heartbeat.instanceId()
                    ))
                    .findFirst()
                    .orElse(null));

            assertEquals(CENTRAL.name(), waitRows(1).getFirst().mode());

            loads.set(new Loads(0.90, 0.20));
            var toAgent = waitPending(client, base, target, AGENT);
            assertEquals(CENTRAL, toAgent.appliedMode());
            assertEquals("CENTRAL_BUSY_AGENT_AVAILABLE", toAgent.commandReason());
            assertEquals(1, rows().size());

            append(source, offloaded);
            waitApplied(client, base, target, toAgent.command());
            assertEquals(AGENT.name(), waitRows(2).get(1).mode());

            loads.set(new Loads(0.20, 0.90));
            var toCentral = waitPending(client, base, target, CENTRAL);
            assertEquals(AGENT, toCentral.appliedMode());
            assertEquals("AGENT_BUSY_CENTRAL_AVAILABLE", toCentral.commandReason());
            assertEquals(toAgent.controllerInstanceId(), toCentral.controllerInstanceId());
            assertEquals(
                    toAgent.command().id().commandVersion() + 1L,
                    toCentral.command().id().commandVersion()
            );
            assertEquals(2, rows().size());

            append(source, returned);
            waitApplied(client, base, target, toCentral.command());

            var stored = waitRows(3);
            assertEquals(
                    List.of("CENTRAL", "AGENT", "CENTRAL"),
                    stored.stream().map(StoredRow::mode).toList()
            );
            assertEquals(3L, stored.stream().map(StoredRow::eventId).distinct().count());
            assertEquals(baseline, mapper.readTree(stored.get(0).payload()).path("raw").stringValue());
            assertEquals(offloaded, mapper.readTree(stored.get(1).payload()).path("raw").stringValue());
            assertEquals(returned, mapper.readTree(stored.get(2).payload()).path("raw").stringValue());
        }
    }

    private SystemStatusResponse centralSample() {
        long now = System.currentTimeMillis();
        return new SystemStatusResponse(
                centralMetricsId, "FRESH", now, 1_000L, 20_000L, 1L,
                now, 0L, 0L, null, loads.get().central(), null, null, null, null
        );
    }

    private void startAgent(String base, Path source) {
        agent = Thread.ofPlatform().daemon().name("auto-mode-http-test-agent").start(() -> {
            try {
                AgentMain.main(new String[]{
                        runId, "source-01", CENTRAL.name(), source.toString(),
                        base + "/api/logs/batches", "100", directory.resolve("queue").toString()
                });
            } catch (InterruptedException stopped) {
                Thread.currentThread().interrupt();
            } catch (Throwable failure) {
                agentFailure.set(failure);
            }
        });
    }

    private static void append(Path path, String raw) throws Exception {
        Files.writeString(path, raw + "\n", StandardCharsets.UTF_8, StandardOpenOption.APPEND);
    }

    private StatusResponse waitPending(
            HttpClient client, String base, Target target, ParsingMode nextMode
    ) throws Exception {
        return await("자동 명령 적용 대기 " + nextMode, () -> {
            var status = readStatus(client, base, target);
            if (status == null || status.command() == null || status.command().mode() != nextMode) {
                return null;
            }

            assertNotEquals(APPLIED, status.phase(), "새 로그를 넣기 전에 명령이 적용됐다.");
            return status.phase() == PENDING_APPLY ? status : null;
        });
    }

    private void waitApplied(HttpClient client, String base, Target target, Command command)
            throws Exception {
        await("자동 명령 적용 " + command.id(), () -> {
            var status = readStatus(client, base, target);
            if (status == null || !command.equals(status.command()) || status.phase() != APPLIED) {
                return null;
            }

            assertEquals(command.mode(), status.appliedMode());
            return status;
        });
    }

    private StatusResponse readStatus(HttpClient client, String base, Target target)
            throws Exception {
        // 이 테스트의 식별자는 URL 쿼리에 그대로 넣을 수 있는 문자열이다.
        String url = base + "/api/agents/control?run_id=" + target.runId()
                + "&source_id=" + target.sourceId() + "&instance_id=" + target.instanceId();
        var request = HttpRequest.newBuilder(URI.create(url))
                .timeout(Duration.ofSeconds(2))
                .GET()
                .build();
        var response = client.send(request, HttpResponse.BodyHandlers.ofByteArray());

        if (response.statusCode() == 404) {
            return null;
        }
        assertEquals(200, response.statusCode());
        return mapper.readValue(response.body(), StatusResponse.class);
    }

    private List<StoredRow> rows() {
        return jdbc.query("""
                SELECT event_id, mode, payload_json
                FROM log_queue
                WHERE run_id = ?
                ORDER BY id
                """,
                (row, index) -> new StoredRow(
                        row.getString("event_id"),
                        row.getString("mode"),
                        row.getString("payload_json")
                ),
                runId
        );
    }

    private List<StoredRow> waitRows(int count) throws Exception {
        return await("SQLite 이벤트 " + count + "건", () -> {
            var stored = rows();
            assertTrue(stored.size() <= count, "예상보다 많은 로그가 보관됐다.");
            return stored.size() == count ? stored : null;
        });
    }

    private <T> T await(String description, Callable<T> probe) throws Exception {
        long startedAt = System.nanoTime();
        while (System.nanoTime() - startedAt < Duration.ofSeconds(20).toNanos()) {
            Throwable failure = agentFailure.get();
            if (failure != null) {
                throw new AssertionError("에이전트 실행 실패", failure);
            }
            assertTrue(agent.isAlive(), "에이전트가 먼저 종료됐다.");

            T result = probe.call();
            if (result != null) {
                return result;
            }
            Thread.sleep(100);
        }
        throw new AssertionError("대기 시간 초과: " + description);
    }

    private record Loads(double central, double agent) {
    }

    private record StoredRow(String eventId, String mode, String payload) {
    }
}