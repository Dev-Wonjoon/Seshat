package com.seshat.monitoring.controller;

import com.seshat.monitoring.AgentStatusService;
import com.seshat.monitoring.dto.AgentHeartbeatRequest;
import com.seshat.monitoring.dto.AgentOverviewResponse;
import com.seshat.monitoring.dto.AgentStatusResponse;
import jakarta.validation.Valid;
import org.springframework.http.CacheControl;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/agents")
public class AgentStatusController {

    private final AgentStatusService statuses;

    public AgentStatusController(AgentStatusService statuses) {
        this.statuses = statuses;
    }

    @PostMapping(
            value = "/heartbeat",
            consumes = MediaType.APPLICATION_JSON_VALUE
    )
    public ResponseEntity<Void> heartbeat(
            @Valid @RequestBody AgentHeartbeatRequest request
    ) {
        statuses.receive(request);
        return ResponseEntity.noContent().build();
    }

    @GetMapping(produces = MediaType.APPLICATION_JSON_VALUE)
    public AgentStatusResponse list(
            @RequestParam("run_id") String runId
    ) {
        return statuses.list(runId);
    }

    @GetMapping(
            value = "/overview",
            produces = MediaType.APPLICATION_JSON_VALUE
    )
    public ResponseEntity<AgentOverviewResponse> overview() {
        return ResponseEntity.ok()
                .cacheControl(
                        CacheControl.noStore()
                )
                .body(statuses.overview());
    }

}