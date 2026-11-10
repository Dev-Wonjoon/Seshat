package com.seshat.monitoring.dto;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

public record AgentStatusResponse(
        @JsonProperty("run_id")
        String runId,

        @JsonProperty("stale_after_ms")
        long staleAfterMs,

        List<Agent> agents
) {
    public record Agent(
            String status,

            @JsonProperty("last_received_at_ms")
            long lastReceivedAtMs,

            @JsonProperty("age_ms")
            long ageMs,

            AgentHeartbeatRequest heartbeat
    ) {
    }
}