package com.seshat.logs.dto;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

public record LogSearchResponse(
        @JsonProperty("run_id")
        String runId,

        int page,

        int size,

        @JsonProperty("total_count")
        long totalCount,

        List<Item> items
) {
    public record Item(
            @JsonProperty("document_id")
            String documentId,

            @JsonProperty("run_id")
            String runId,

            @JsonProperty("source_id")
            String sourceId,

            @JsonProperty("event_id")
            String eventId,

            String mode,

            @JsonProperty("parse_status")
            String parseStatus,

            @JsonProperty("raw_preview")
            String rawPreview
    ) {
    }
}