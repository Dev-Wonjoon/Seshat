package com.seshat.logs.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import tools.jackson.databind.node.ObjectNode;

public record LogDetailResponse(
        @JsonProperty("document_id")
        String documentId,

        ObjectNode document
) {
}