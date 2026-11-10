package com.seshat.monitoring.controller;

import com.seshat.monitoring.SystemStatusService;
import com.seshat.monitoring.dto.SystemStatusResponse;
import org.springframework.http.CacheControl;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping(
        value = "/api/system",
        produces = MediaType.APPLICATION_JSON_VALUE
)
public class SystemStatusController {

    private final SystemStatusService statuses;

    public SystemStatusController(SystemStatusService statuses) {
        this.statuses = statuses;
    }

    @GetMapping("/status")
    public ResponseEntity<SystemStatusResponse> status() {
        return ResponseEntity.ok()
                .cacheControl(CacheControl.noStore())
                .body(statuses.snapshot());
    }
}