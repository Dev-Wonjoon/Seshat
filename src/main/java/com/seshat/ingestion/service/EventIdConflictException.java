package com.seshat.ingestion.service;

import java.util.UUID;

public class EventIdConflictException extends RuntimeException {
    public EventIdConflictException(
            String runId,
            String sourceId,
            UUID eventId
    ) {
        super("Event ID conflict: " + runId + "/" + sourceId + "/" + eventId);
    }
}
