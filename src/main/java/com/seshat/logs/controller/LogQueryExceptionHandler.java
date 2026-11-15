package com.seshat.logs.controller;

import co.elastic.clients.elasticsearch._types.ElasticsearchException;
import com.seshat.ingestion.dto.ApiError;
import com.seshat.logs.service.LogNotFoundException;
import com.seshat.logs.service.LogSnapshotExpiredException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.MissingServletRequestParameterException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.method.annotation.MethodArgumentTypeMismatchException;

import java.io.IOException;

@RestControllerAdvice(assignableTypes = LogQueryController.class)
public class LogQueryExceptionHandler {

    private static final Logger log =
            LoggerFactory.getLogger(LogQueryExceptionHandler.class);

    @ExceptionHandler({
            MissingServletRequestParameterException.class,
            MethodArgumentTypeMismatchException.class,
            IllegalArgumentException.class
    })
    public ResponseEntity<ApiError> invalidRequest() {
        return ResponseEntity.badRequest().body(
                new ApiError(
                        "INVALID_REQUEST",
                        "Check the required parameters, filter values, and page range."
                )
        );
    }

    @ExceptionHandler(LogNotFoundException.class)
    public ResponseEntity<ApiError> notFound() {
        return ResponseEntity.status(HttpStatus.NOT_FOUND).body(
                new ApiError(
                        "LOG_NOT_FOUND",
                        "The requested log document was not found."
                )
        );
    }

    @ExceptionHandler(LogSnapshotExpiredException.class)
    public ResponseEntity<ApiError> snapshotExpired() {
        return ResponseEntity.status(HttpStatus.GONE)
                .body(new ApiError("LOG_SNAPSHOT_EXPIRED", "로그 검색이 만료됐습니다."));
    }

    @ExceptionHandler({
            IOException.class,
            ElasticsearchException.class
    })
    public ResponseEntity<ApiError> storageFailure(Exception exception) {
        log.error("Failed to query stored logs", exception);

        return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                .body(new ApiError(
                        "STORAGE_UNAVAILABLE",
                        "Stored logs could not be read."
                ));
    }
}