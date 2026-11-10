package com.seshat.monitoring.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.UUID;

@JsonInclude(JsonInclude.Include.ALWAYS)
public record SystemStatusResponse(
        @JsonProperty("instance_id")
        UUID instanceId,

        @JsonProperty("metrics_status")
        String metricsStatus,

        @JsonProperty("server_time_ms")
        long serverTimeMs,

        @JsonProperty("sample_interval_ms")
        long sampleIntervalMs,

        @JsonProperty("stale_after_ms")
        long staleAfterMs,

        @JsonProperty("sample_sequence")
        long sampleSequence,

        @JsonProperty("sampled_at_ms")
        Long sampledAtMs,

        @JsonProperty("sample_age_ms")
        Long sampleAgeMs,

        @JsonProperty("uptime_ms")
        Long uptimeMs,

        @JsonProperty("process_cpu_load")
        Double processCpuLoad,

        @JsonProperty("system_cpu_load")
        Double systemCpuLoad,

        @JsonProperty("heap_used_bytes")
        Long heapUsedBytes,

        @JsonProperty("heap_max_bytes")
        Long heapMaxBytes
) {
}