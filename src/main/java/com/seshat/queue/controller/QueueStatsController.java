package com.seshat.queue.controller;

import com.seshat.ingestion.dto.QueueStatsResponse;
import com.seshat.queue.QueueStatsService;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/queue")
public class QueueStatsController {

    private final QueueStatsService stats;

    public QueueStatsController(QueueStatsService stats) {
        this.stats = stats;
    }

    @GetMapping(value = "/stats", produces = MediaType.APPLICATION_JSON_VALUE)
    public QueueStatsResponse getStats(@RequestParam("run_id") String runId) {
        return stats.getStats(runId);
    }
}
