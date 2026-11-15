package com.seshat.repair;

public record RepairCandidate(
        Decision decision,
        String candidateJson,
        String reason,
        String responseText
) {
    public enum Decision {
        CANDIDATE,
        UNRECOVERABLE
    }
}
