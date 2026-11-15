package com.seshat.queue;

import com.seshat.ingestion.dto.QueueStatsResponse;
import com.seshat.ingestion.dto.QueueTrafficResponse;

import java.util.Arrays;
import java.util.List;
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

    private static final long BUCKET_MS = 5_000;
    private static final int BUCKET_COUNT = 180;

    private final JdbcTemplate jdbcTemplate;

    public QueueStatsService(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public long countParseFailures(String runId) {
        return jdbcTemplate.queryForObject("""
                SELECT COUNT(*)
                FROM log_queue
                WHERE run_id = ?
                  AND CASE WHEN json_valid(result_json)
                           THEN json_extract(result_json, '$.parse_status') = 'FAILED'
                           ELSE FALSE END
                """, Long.class, runId);
    }

    public QueueTrafficResponse getTraffic() {
        long end = Math.floorDiv(System.currentTimeMillis(), BUCKET_MS) * BUCKET_MS;
        long start = end - BUCKET_COUNT * BUCKET_MS;
        var counts = new Long[BUCKET_COUNT];
        Arrays.fill(counts, 0L);

        // 완료된 5초 구간만 집계해 마지막 구간의 유입률이 작게 보이지 않게 한다.
        jdbcTemplate.query("""
                SELECT ((received_at_ms - ?) / ?) AS bucket, COUNT(*) AS events
                FROM log_queue
                WHERE received_at_ms >= ? AND received_at_ms < ?
                GROUP BY bucket
                """, row -> {
            counts[row.getInt("bucket")] = row.getLong("events");
        }, start, BUCKET_MS, start, end);

        return jdbcTemplate.queryForObject("""
                SELECT
                    COUNT(CASE WHEN state = 'PENDING' THEN 1 END) AS pending,
                    COUNT(CASE WHEN state = 'PROCESSING' THEN 1 END) AS processing,
                    COUNT(CASE WHEN state = 'RETRY_WAIT' THEN 1 END) AS retry_wait
                FROM log_queue
                WHERE state IN ('PENDING', 'PROCESSING', 'RETRY_WAIT')
                """, (row, rowNum) -> new QueueTrafficResponse(
                start, end, BUCKET_MS, row.getLong("pending"), row.getLong("processing"),
                row.getLong("retry_wait"), List.of(counts)));
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
