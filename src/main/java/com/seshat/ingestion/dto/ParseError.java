package com.seshat.ingestion.dto;

import jakarta.validation.constraints.NotBlank;

public record ParseError(
        @NotBlank String code,
        @NotBlank String message
) {
}
