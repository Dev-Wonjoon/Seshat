package com.seshat.queue;

import com.seshat.ingestion.dto.ParsingMode;
import tools.jackson.databind.node.ObjectNode;

import java.util.UUID;

public record PreparedLogEvent(
        long queueId,
        String runId,
        String sourceId,
        UUID eventId,
        ParsingMode mode,
        ObjectNode result,
        int attemptCount
) {
}
