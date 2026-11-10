package com.seshat.storage.elasticsearch;

public record BulkItemResult(
        long queueId,
        String documentId,
        boolean successful,
        int status,
        String errorType,
        String errorReason
) {
}
