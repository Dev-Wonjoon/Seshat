package com.seshat.queue.controller;

import com.seshat.ingestion.dto.ApiError;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.MissingServletRequestParameterException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

@RestControllerAdvice(assignableTypes = QueueStatsController.class)
public class QueueStatsExceptionHandler {

    private static final Logger log = LoggerFactory.getLogger(QueueStatsExceptionHandler.class);

    @ExceptionHandler({
            MissingServletRequestParameterException.class,
            IllegalArgumentException.class
    })
    public ResponseEntity<ApiError> invalidRequest() {
        return ResponseEntity.badRequest().body(
                new ApiError("INVALID_REQUEST",
                        "A non-blank run_id query parameter is required."
                )
        );
    }

    @ExceptionHandler(DataAccessException.class)
    public ResponseEntity<ApiError> storageFailure(DataAccessException e) {
        log.error("Failed to read queue statistics", e);

        return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                .body(new ApiError(
                        "STORAGE_UNAVAILABLE",
                        "Queue statistics could not be read."
                ));
    }
}
