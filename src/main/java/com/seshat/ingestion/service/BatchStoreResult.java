package com.seshat.ingestion.service;

public record BatchStoreResult(
        int accountedCount,
        int insertedCount,
        int duplicateCount
) {
}
