package com.seshat.ingestion.dto;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

public record QueueTrafficResponse(
        @JsonProperty("window_start_ms") long windowStartMs,
        @JsonProperty("window_end_ms") long windowEndMs,
        @JsonProperty("bucket_ms") long bucketMs,
        @JsonProperty("queued_count") long queuedCount,
        @JsonProperty("processing_count") long processingCount,
        @JsonProperty("retry_wait_count") long retryWaitCount,
        List<Long> counts
) {
}
