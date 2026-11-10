package com.seshat.ingestion;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;

import static org.junit.jupiter.api.Assertions.assertEquals;

@SpringBootTest(properties = {"spring.datasource.url=jdbc:sqlite::memory:"})
public class SqliteSchemaTest {

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Test
    void 시작할_때_큐_테이블을_생성한다() {
        Integer count = jdbcTemplate.queryForObject(
                """
                SELECT COUNT(*)
                FROM sqlite_master
                WHERE type = 'table'
                    AND name = 'log_queue'
                """,
                Integer.class
        );

        assertEquals(Integer.valueOf(1), count);
    }
}
