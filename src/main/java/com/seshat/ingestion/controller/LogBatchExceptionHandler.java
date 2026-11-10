package com.seshat.ingestion.controller;

import com.seshat.ingestion.dto.ApiError;
import com.seshat.ingestion.service.EventIdConflictException;
import jakarta.validation.ConstraintViolationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.transaction.TransactionException;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

@RestControllerAdvice(assignableTypes = LogBatchController.class)
public class LogBatchExceptionHandler {

    private static final Logger log = LoggerFactory.getLogger(LogBatchExceptionHandler.class);

    @ExceptionHandler({
            MethodArgumentNotValidException.class,
            HttpMessageNotReadableException.class,
            ConstraintViolationException.class
    })
    public ResponseEntity<ApiError> invalidRequest() {
        return ResponseEntity.badRequest().body(
                new ApiError(
                        "INVALID_REQUEST",
                        "The request body or event fields are invalid."
                )
        );
    }

    @ExceptionHandler(EventIdConflictException.class)
    public ResponseEntity<ApiError> eventConflict() {
        return ResponseEntity.status(HttpStatus.CONFLICT).body(
                new ApiError(
                        "EVENT_ID_CONFLICT",
                        "An event ID is already stored with different content or mode."
                )
        );
    }

    @ExceptionHandler({
            DataAccessException.class,
            TransactionException.class
    })
    public ResponseEntity<ApiError> storageFailure(Exception exception) {
        log.error("Failed to persist log batch.", exception);

        return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).body(
                new ApiError(
                        "STORAGE_UNAVAILABLE",
                        "The batch could not be committed. Retry with the same IDs and content."
                )
        );
    }
}
