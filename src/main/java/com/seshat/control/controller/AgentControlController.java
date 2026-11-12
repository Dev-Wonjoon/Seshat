package com.seshat.control.controller;

import com.seshat.control.AgentControlService;
import com.seshat.control.AgentControlService.ControlException;
import com.seshat.control.dto.AgentControlMessages.Command;
import com.seshat.control.dto.AgentControlMessages.CreateCommandRequest;
import com.seshat.control.dto.AgentControlMessages.StatusResponse;
import com.seshat.control.dto.AgentControlMessages.SyncRequest;
import com.seshat.control.dto.AgentControlMessages.SyncResponse;
import com.seshat.control.dto.AgentControlMessages.Target;
import com.seshat.ingestion.dto.ApiError;
import jakarta.validation.Valid;
import org.springframework.http.CacheControl;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.MissingServletRequestParameterException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.method.annotation.MethodArgumentTypeMismatchException;

import java.util.UUID;

@RestController
@RequestMapping(value = "/api/agents/control", produces = MediaType.APPLICATION_JSON_VALUE)
public class AgentControlController {

    private final AgentControlService control;

    public AgentControlController(AgentControlService control) {
        this.control = control;
    }

    @PostMapping(value = "/sync", consumes = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<SyncResponse> sync(@Valid @RequestBody SyncRequest request) {
        return ResponseEntity.ok()
                .cacheControl(CacheControl.noStore())
                .body(control.sync(request));
    }

    @PostMapping(value = "/commands", consumes = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<Command> issue(@Valid @RequestBody CreateCommandRequest request) {
        return ResponseEntity.ok()
                .cacheControl(CacheControl.noStore())
                .body(control.issue(request));
    }

    @GetMapping
    public ResponseEntity<StatusResponse> status(
            @RequestParam("run_id") String runId,
            @RequestParam("source_id") String sourceId,
            @RequestParam("instance_id") UUID instanceId
    ) {
        return ResponseEntity.ok()
                .cacheControl(CacheControl.noStore())
                .body(control.status(new Target(runId, sourceId, instanceId)));
    }

    @ExceptionHandler(ControlException.class)
    public ResponseEntity<ApiError> controlError(ControlException error) {
        return ResponseEntity.status(error.status())
                .body(new ApiError(error.code(), error.getMessage()));
    }

    @ExceptionHandler({
            MethodArgumentNotValidException.class,
            HttpMessageNotReadableException.class,
            MissingServletRequestParameterException.class,
            MethodArgumentTypeMismatchException.class
    })
    public ResponseEntity<ApiError> invalidRequest() {
        return ResponseEntity.badRequest()
                .body(new ApiError("INVALID_REQUEST", "제어 요청의 필드와 형식을 확인하세요."));
    }
}