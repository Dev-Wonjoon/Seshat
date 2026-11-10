package com.seshat.ingestion.dto;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.Valid;
import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

public record LogBatchRequest(
        @JsonProperty("run_id")
        @NotBlank
        String runId,

        @JsonProperty("source_id")
        @NotBlank
        String sourceId,

        @NotNull
        ParsingMode mode,

        @NotNull
        @Size(min = 1, max = 50)
        @Valid
        List<@NotNull LogEventRequest> events
) {

    @JsonIgnore
    @AssertTrue(message = "Event fields do not match the parsing mode.")
    public boolean isModeFieldsValid() {
        if(mode == null || events == null) {
            return true;
        }

        return events.stream().allMatch(
                event -> event == null || matchesMode(event)
        );
    }

    private boolean matchesMode(LogEventRequest event) {
        boolean noParsed = event.parsed() == null
                || event.parsed().isNull();

        if(mode == ParsingMode.CENTRAL) {
            return event.parseStatus() == null
                    && noParsed
                    && event.parseError() == null;
        }

        if(event.parseStatus() == null) {
            return false;
        }

        return switch (event.parseStatus()) {
            case SUCCESS -> !noParsed
                    && event.parsed().isObject()
                    && event.parseError() == null;

            case FAILED -> noParsed && event.parseError() != null;
        };
    }

    @JsonIgnore
    @AssertTrue(message = "Event IDs must be unique within a batch.")
    public boolean isEventIdsUnique() {
        if(events == null) {
            return true;
        }

        Set<UUID> ids = new HashSet<>();

        for(LogEventRequest event : events) {
            if(event == null || event.eventId() == null) {
                continue;
            }

            if(!ids.add(event.eventId())) {
                return false;
            }
        }

        return true;
    }
}
