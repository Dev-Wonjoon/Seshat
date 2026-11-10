package com.seshat.ingestion.dto;

public record ApiError(
        String code,
        String message
) {
}
