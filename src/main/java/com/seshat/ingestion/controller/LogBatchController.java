package com.seshat.ingestion.controller;

import com.seshat.ingestion.dto.LogBatchRequest;
import com.seshat.ingestion.dto.LogBatchResponse;
import com.seshat.ingestion.service.LogBatchStorageService;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/logs/batches")
public class LogBatchController {

    private final LogBatchStorageService storageService;

    public LogBatchController(LogBatchStorageService storageService) {
        this.storageService = storageService;
    }

    @PostMapping(
            consumes = MediaType.APPLICATION_JSON_VALUE,
            produces = MediaType.APPLICATION_JSON_VALUE
    )
    public ResponseEntity<LogBatchResponse> receive(
            @Valid @RequestBody LogBatchRequest request
    ) {
        var result = storageService.store(request);

        var response = new LogBatchResponse(
                result.accountedCount(),
                result.insertedCount(),
                result.duplicateCount()
        );

        return ResponseEntity.status(HttpStatus.ACCEPTED).body(response);
    }
}
