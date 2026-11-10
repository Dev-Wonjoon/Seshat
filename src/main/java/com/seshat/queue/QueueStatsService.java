package com.seshat.queue;

import com.seshat.ingestion.dto.QueueStatsResponse;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

@Service
public class QueueStatsService {
    private static final String STATS_SQL = """
        SELECT
            COUNT(*) AS total_count,
            COUNT (CASE WHEN state = 'PENDING' THEN 1 END) AS pending_count,
            COUNT (CASE WHEN state = 'PROCESSING' THEN 1 END) AS processing_count,
            COUNT (CASE WHEN state = 'RETRY_WAIT' THEN 1 END) AS retry_wait_count,
            COUNT (CASE WHEN state = 'COMPLETED' THEN 1 END) AS completed_count,
            COUNT (CASE WHEN state = 'STORAGE_FAILED' THEN 1 END) AS storage_failed_count
        FROM log_queue
        WHERE run_id = ?
    """;

    private final JdbcTemplate jdbcTemplate;

    public QueueStatsService(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public QueueStatsResponse getStats(String runId) {
        if(runId == null || runId.isBlank()) {
            throw new IllegalArgumentException("run_id must not be blank.");
        }

        return jdbcTemplate.queryForObject(
                STATS_SQL,
                (resultSet, rowNum) -> new QueueStatsResponse(
                        runId,
                        resultSet.getLong("total_count"),
                        resultSet.getLong("pending_count"),
                        resultSet.getLong("processing_count"),
                        resultSet.getLong("retry_wait_count"),
                        resultSet.getLong("completed_count"),
                        resultSet.getLong("storage_failed_count")
                ),
                runId
        );
    }
}
