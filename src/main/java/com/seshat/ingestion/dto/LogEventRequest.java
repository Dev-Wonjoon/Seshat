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
        ParseError parseError,
        @JsonProperty("log_format")
        LogFormat logFormat
) {
    public LogEventRequest {
        if (logFormat == LogFormat.JSON) {
            logFormat = null;
        }
    }

    public LogEventRequest(
            UUID eventId, String raw, ParseStatus parseStatus, JsonNode parsed, ParseError parseError
    ) {
        this(eventId, raw, parseStatus, parsed, parseError, null);
    }
}
