package com.seshat.logs.service;

public final class LogNotFoundException extends RuntimeException {

    public LogNotFoundException() {
        super("Log document was not found.");
    }
}