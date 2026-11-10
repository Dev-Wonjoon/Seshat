package com.seshat.queue;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class QueueRecoveryService {

    private static final String RECOVER_SQL = """
            UPDATE log_queue
            SET state = 'PENDING',
                processing_started_at_ms = NULL,
                next_attempt_at_ms = 0
            WHERE state = 'PROCESSING'
            """;

    private final JdbcTemplate jdbcTemplate;

    public QueueRecoveryService(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    /**
     * 이전 프로세스가 종료되고 새 작업자가 시작하기 전에 호출한다.
     * 회수한 이벤트 수를 반환한다.
     */
    @Transactional
    public int recoverInterrupted() {
        return jdbcTemplate.update(RECOVER_SQL);
    }
}