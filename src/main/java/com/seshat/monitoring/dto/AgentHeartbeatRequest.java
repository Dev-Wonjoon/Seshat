package com.seshat.monitoring.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.seshat.ingestion.dto.ParsingMode;
import jakarta.validation.constraints.DecimalMax;
import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import jakarta.validation.constraints.PositiveOrZero;

import java.util.UUID;

@JsonInclude(JsonInclude.Include.ALWAYS)
public record AgentHeartbeatRequest(
        @JsonProperty("run_id")
        @NotBlank
        String runId,

        @JsonProperty("source_id")
        @NotBlank
        String sourceId,

        @JsonProperty("instance_id")
        @NotNull
        UUID instanceId,

        @JsonProperty("applied_mode")
        @NotNull
        ParsingMode appliedMode,

        @JsonProperty("process_cpu_load")
        @DecimalMin("0.0")
        @DecimalMax("1.0")
        Double processCpuLoad,

        @JsonProperty("system_cpu_load")
        @DecimalMin("0.0")
        @DecimalMax("1.0")
        Double systemCpuLoad,

        @JsonProperty("heap_used_bytes")
        @PositiveOrZero
        Long heapUsedBytes,

        @JsonProperty("heap_max_bytes")
        @Positive
        Long heapMaxBytes,

        @JsonProperty("pending_batch_count")
        @NotNull
        @PositiveOrZero
        Integer pendingBatchCount,

        @JsonProperty("pending_bytes")
        @NotNull
        @PositiveOrZero
        Long pendingBytes,

        @JsonProperty("max_pending_batches")
        @NotNull
        @Positive
        Integer maxPendingBatches,

        @JsonProperty("max_pending_bytes")
        @NotNull
        @Positive
        Long maxPendingBytes
) {
}