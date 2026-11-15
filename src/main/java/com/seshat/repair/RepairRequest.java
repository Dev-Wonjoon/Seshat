package com.seshat.repair;

import java.time.Duration;

public record RepairRequest(
        String raw,
        String errorCode,
        String errorMessage,
        Duration timeout
) {
}
