package com.seshat.logs.controller;

import com.seshat.ingestion.dto.ParseStatus;
import com.seshat.ingestion.dto.ParsingMode;
import com.seshat.logs.dto.LogChunkRequest;
import com.seshat.logs.dto.LogChunkResponse;
import com.seshat.logs.dto.LogDetailResponse;
import com.seshat.logs.dto.LogSearchResponse;
import com.seshat.logs.dto.LogRunIds;
import com.seshat.logs.service.LogQueryService;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;

@RestController
@RequestMapping(
        value = "/api/logs",
        produces = MediaType.APPLICATION_JSON_VALUE
)
public class LogQueryController {

    private final LogQueryService queries;

    public LogQueryController(LogQueryService queries) {
        this.queries = queries;
    }

    @GetMapping
    public LogSearchResponse search(
            @RequestParam("run_id") String runId,
            @RequestParam(
                    value = "source_id",
                    required = false
            ) String sourceId,
            @RequestParam(
                    value = "mode",
                    required = false
            ) ParsingMode mode,
            @RequestParam(
                    value = "parse_status",
                    required = false
            ) ParseStatus parseStatus,
            @RequestParam(
                    value = "q",
                    required = false
            ) String queryText,
            @RequestParam(
                    value = "page",
                    defaultValue = "0"
            ) int page,
            @RequestParam(
                    value = "size",
                    defaultValue = "20"
            ) int size
    ) throws IOException {
        return queries.search(
                runId,
                sourceId,
                mode,
                parseStatus,
                queryText,
                page,
                size
        );
    }

    @GetMapping("/run-ids")
    public LogRunIds runIds(
            @RequestParam(value = "q", required = false) String query,
            @RequestParam(value = "after", required = false) String after
    ) throws IOException {
        return queries.runIds(query, after);
    }

    @PostMapping(value = "/chunks", consumes = MediaType.APPLICATION_JSON_VALUE)
    public LogChunkResponse chunk(@RequestBody LogChunkRequest request) throws IOException {
        return queries.chunk(request);
    }

    @PostMapping(value = "/chunks/close", consumes = MediaType.APPLICATION_JSON_VALUE)
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void closeSnapshot(@RequestBody LogChunkRequest.CloseSnapshot request) throws IOException {
        queries.closeSnapshot(request.pitId());
    }

    @GetMapping("/{documentId}")
    public LogDetailResponse detail(
            @PathVariable("documentId") String documentId,
            @RequestParam("run_id") String runId
    ) throws IOException {
        return queries.detail(documentId, runId);
    }
}