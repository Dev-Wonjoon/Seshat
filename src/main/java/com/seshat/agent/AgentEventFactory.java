package com.seshat.agent;

import com.seshat.ingestion.dto.*;
import com.seshat.processing.JavaLogProcessor;

import java.util.Objects;
import java.util.UUID;

public final class AgentEventFactory {

    private final JavaLogProcessor processor;
    private final LogFormat logFormat;

    public AgentEventFactory(JavaLogProcessor processor) {
        this(processor, LogFormat.JSON);
    }

    public AgentEventFactory(JavaLogProcessor processor, LogFormat logFormat) {
        this.processor = processor;
        this.logFormat = Objects.requireNonNull(logFormat);
    }

    public LogEventRequest create(String raw, ParsingMode mode) {
        return create(UUID.randomUUID(), raw, mode);
    }

    /**
     * 지정된 이벤트 ID를 유지하면서 해당 모드의 전송 내용을 만든다.
     */
    public LogEventRequest create(
            UUID eventId,
            String raw,
            ParsingMode mode
    ) {
        Objects.requireNonNull(eventId);
        Objects.requireNonNull(mode);

        if (mode == ParsingMode.CENTRAL) {
            return new LogEventRequest(
                    eventId, raw, null, null, null, logFormat
            );
        }

        var result = processor.process(raw, logFormat);
        var status = ParseStatus.valueOf(
                result.path("parse_status").asText()
        );

        if (status == ParseStatus.SUCCESS) {
            return new LogEventRequest(
                    eventId,
                    raw,
                    status,
                    result.get("parsed"),
                    null,
                    logFormat
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
                ),
                logFormat
        );
    }
}
