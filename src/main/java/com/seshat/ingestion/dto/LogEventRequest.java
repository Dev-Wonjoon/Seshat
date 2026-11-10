package com.seshat.ingestion.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import tools.jackson.databind.JsonNode;

import java.util.UUID;

@JsonInclude(JsonInclude.Include.NON_NULL)
public record LogEventRequest(
        @JsonProperty("event_id")
        @NotNull
        UUID eventId,

        @NotNull
        String raw,

        @JsonProperty("parse_status")
        ParseStatus parseStatus,

        JsonNode parsed,

        @JsonProperty("parse_error")
        @Valid
        ParseError parseError
) {
}
