package com.seshat.logs.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.seshat.ingestion.dto.ParseStatus;
import com.seshat.ingestion.dto.ParsingMode;

public record LogChunkRequest(
        @JsonProperty("run_id") String runId,
        @JsonProperty("source_id") String sourceId,
        ParsingMode mode,
        @JsonProperty("parse_status") ParseStatus parseStatus,
        @JsonProperty("q") String query,
        @JsonProperty("pit_id") String pitId,
        LogChunkResponse.Cursor after
) {
    public record CloseSnapshot(@JsonProperty("pit_id") String pitId) {}
}
