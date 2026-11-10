package com.seshat.monitoring.dto;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

public record AgentOverviewResponse(
        @JsonProperty("stale_after_ms")
        long staleAfterMs,

        List<AgentStatusResponse.Agent> agents
) {
}