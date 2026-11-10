package com.seshat.monitoring;

import com.seshat.monitoring.dto.AgentHeartbeatRequest;
import com.seshat.monitoring.dto.AgentOverviewResponse;
import com.seshat.monitoring.dto.AgentStatusResponse;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.Comparator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

@Service
public class AgentStatusService {

    private final long staleAfterMs;

    private final ConcurrentMap<AgentKey, Observation> latest =
            new ConcurrentHashMap<>();

    public AgentStatusService(
            @Value("${seshat.monitoring.stale-after-ms:20000}")
            long staleAfterMs
    ) {
        if (staleAfterMs <= 0) {
            throw new IllegalArgumentException(
                    "stale-after-ms must be positive."
            );
        }

        this.staleAfterMs = staleAfterMs;
    }

    public void receive(AgentHeartbeatRequest heartbeat) {
        var key = new AgentKey(
                heartbeat.runId(),
                heartbeat.sourceId()
        );

        latest.compute(key, (unused, previous) -> new Observation(
                heartbeat,
                System.currentTimeMillis(),
                System.nanoTime()
        ));
    }

    public AgentStatusResponse list(String runId) {
        if (runId == null || runId.isBlank()) {
            throw new IllegalArgumentException(
                    "run_id must not be blank."
            );
        }

        long now = System.nanoTime();

        var agents = latest.entrySet().stream()
                .filter(entry -> entry.getKey().runId().equals(runId))
                .map(entry -> status(entry.getValue(), now))
                .sorted(Comparator.comparing(
                        agent -> agent.heartbeat().sourceId()
                ))
                .toList();

        return new AgentStatusResponse(
                runId,
                staleAfterMs,
                agents
        );
    }

    public AgentOverviewResponse overview() {
        long now = System.nanoTime();

        var agents = latest.values().stream()
                .map(observation -> status(observation, now))
                .sorted(
                        Comparator.comparing(
                                (AgentStatusResponse.Agent agent) ->
                                        agent.heartbeat().runId()
                        ).thenComparing(
                                agent -> agent.heartbeat().sourceId()
                        )
                )
                .toList();

        return new AgentOverviewResponse(
                staleAfterMs,
                agents
        );
    }

    private AgentStatusResponse.Agent status(
            Observation observation,
            long now
    ) {
        long ageMs = Math.max(
                0L,
                TimeUnit.NANOSECONDS.toMillis(
                        now - observation.receivedAtNanos()
                )
        );

        String status = ageMs >= staleAfterMs
                ? "STALE"
                : "ONLINE";

        return new AgentStatusResponse.Agent(
                status,
                observation.receivedAtMs(),
                ageMs,
                observation.heartbeat()
        );
    }

    private record AgentKey(
            String runId,
            String sourceId
    ) {
    }

    private record Observation(
            AgentHeartbeatRequest heartbeat,
            long receivedAtMs,
            long receivedAtNanos
    ) {
    }
}