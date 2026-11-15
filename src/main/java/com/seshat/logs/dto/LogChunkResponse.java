package com.seshat.logs.dto;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

public record LogChunkResponse(
        @JsonProperty("pit_id") String pitId,
        @JsonProperty("total_count") Long totalCount,
        List<LogSearchResponse.Item> items,
        @JsonProperty("next_cursor") Cursor nextCursor
) {
    public record Cursor(
            String sequence,
            @JsonProperty("shard_doc") String shardDoc
    ) {}
}
