package com.seshat.ingestion.dto;

import com.fasterxml.jackson.annotation.JsonProperty;

public record QueueStatsResponse(
        @JsonProperty("run_id")
        String runId,

        @JsonProperty("total_count")
        long totalCount,

        @JsonProperty("pending_count")
        long pendingCount,

        @JsonProperty("processing_count")
        long processingCount,

        @JsonProperty("retry_wait_count")
        long retryWaitCount,

        @JsonProperty("completed_count")
        long completedCount,

        @JsonProperty("storage_failed_count")
        long storageFailedCount
) {
}
