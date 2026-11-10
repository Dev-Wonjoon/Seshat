package com.seshat.agent;

import com.seshat.ingestion.dto.*;
import com.seshat.processing.JavaLogProcessor;

import java.util.UUID;

public final class AgentEventFactory {

    private final JavaLogProcessor processor;

    public AgentEventFactory(JavaLogProcessor processor) {
        this.processor = processor;
    }

    public LogEventRequest create(String raw, ParsingMode mode) {
        UUID eventId = UUID.randomUUID();

        if (mode == ParsingMode.CENTRAL) {
            return new LogEventRequest(
                    eventId, raw, null, null, null
            );
        }

        var result = processor.process(raw);
        var status = ParseStatus.valueOf(
                result.path("parse_status").asText()
        );

        if (status == ParseStatus.SUCCESS) {
            return new LogEventRequest(
                    eventId,
                    raw,
                    status,
                    result.get("parsed"),
                    null
            );
        }

        var error = result.path("parse_error");

        return new LogEventRequest(
                eventId,
                raw,
                status,
                null,
                new ParseError(
                        error.path("code").asText(),
                        error.path("message").asText()
                )
        );
    }
}