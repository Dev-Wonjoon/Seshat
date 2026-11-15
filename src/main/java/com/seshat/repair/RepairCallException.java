package com.seshat.repair;

public class RepairCallException extends Exception {

    public enum Reason {
        TIMEOUT,
        CONNECTION,
        SERVER_ERROR,
        AUTHENTICATION,
        INVALID_RESPONSE
    }

    private final Reason reason;

    public RepairCallException(Reason reason) {
        super(reason.name());
        this.reason = reason;
    }

    public Reason reason() {
        return reason;
    }

    public boolean retryable() {
        return switch (reason) {
            case TIMEOUT, CONNECTION, SERVER_ERROR -> true;
            case AUTHENTICATION, INVALID_RESPONSE -> false;
        };
    }
}
