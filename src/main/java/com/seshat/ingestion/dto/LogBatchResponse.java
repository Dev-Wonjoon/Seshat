package com.seshat.ingestion.dto;

import com.fasterxml.jackson.annotation.JsonProperty;

public record LogBatchResponse(
        @JsonProperty("accepted_count")
        int acceptedCount,

        @JsonProperty("inserted_count")
        int insertedCount,

        @JsonProperty("duplicate_count")
        int duplicateCount
) {
}
