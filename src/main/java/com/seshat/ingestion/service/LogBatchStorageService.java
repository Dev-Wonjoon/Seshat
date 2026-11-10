package com.seshat.ingestion.service;

import com.seshat.ingestion.dto.LogBatchRequest;
import com.seshat.ingestion.dto.LogEventRequest;
import jakarta.validation.ConstraintViolationException;
import jakarta.validation.Validator;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import tools.jackson.databind.json.JsonMapper;

import java.util.Objects;

@Service
public class LogBatchStorageService {

    private static final String INSERT_SQL = """
            INSERT INTO log_queue (
                run_id,
                source_id,
                event_id,
                mode,
                payload_json,
                received_at_ms
            )
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT (run_id, source_id, event_id) DO NOTHING
            """;

    private static final String SELECT_SQL = """
            SELECT mode, payload_json
            FROM log_queue
            WHERE run_id = ?
              AND source_id = ?
              AND event_id = ?
            """;

    private final JdbcTemplate jdbcTemplate;
    private final JsonMapper mapper;
    private final Validator validator;

    public LogBatchStorageService(
            JdbcTemplate jdbcTemplate,
            JsonMapper mapper,
            Validator validator
    ) {
        this.jdbcTemplate = jdbcTemplate;
        this.mapper = mapper;
        this.validator = validator;
    }

    @Transactional
    public BatchStoreResult store(LogBatchRequest batch) {
        Objects.requireNonNull(batch, "batch must not be null");

        var violations = validator.validate(batch);

        if (!violations.isEmpty()) {
            throw new ConstraintViolationException(violations);
        }

        long receivedAt = System.currentTimeMillis();
        int insertedCount = 0;
        int duplicateCount = 0;

        for (LogEventRequest event : batch.events()) {
            String payloadJson = serializePayload(event);

            int affectedRows = jdbcTemplate.update(
                    INSERT_SQL,
                    batch.runId(),
                    batch.sourceId(),
                    event.eventId().toString(),
                    batch.mode().name(),
                    payloadJson,
                    receivedAt
            );

            if (affectedRows == 1) {
                insertedCount++;
            } else if (affectedRows == 0) {
                verifyDuplicate(batch, event, payloadJson);
                duplicateCount++;
            } else {
                throw new IllegalStateException(
                        "Unexpected insert count: " + affectedRows
                );
            }
        }

        return new BatchStoreResult(
                batch.events().size(),
                insertedCount,
                duplicateCount
        );
    }

    private String serializePayload(LogEventRequest event) {
        // 선택 필드 parsed의 생략과 명시적 null을 같은 형태로 저장한다.
        var parsed = event.parsed();

        if (parsed != null && parsed.isNull()) {
            parsed = null;
        }

        var normalized = new LogEventRequest(
                event.eventId(),
                event.raw(),
                event.parseStatus(),
                parsed,
                event.parseError()
        );

        return mapper.writeValueAsString(normalized);
    }

    private void verifyDuplicate(
            LogBatchRequest batch,
            LogEventRequest event,
            String payloadJson
    ) {
        StoredEvent existing = jdbcTemplate.queryForObject(
                SELECT_SQL,
                (resultSet, rowNum) -> new StoredEvent(
                        resultSet.getString("mode"),
                        resultSet.getString("payload_json")
                ),
                batch.runId(),
                batch.sourceId(),
                event.eventId().toString()
        );

        if (existing == null) {
            throw new IllegalStateException(
                    "Existing event could not be loaded."
            );
        }

        boolean sameMode = batch.mode().name()
                .equals(existing.mode());

        boolean samePayload = mapper.readTree(existing.payloadJson())
                .equals(mapper.readTree(payloadJson));

        if (!sameMode || !samePayload) {
            throw new EventIdConflictException(
                    batch.runId(),
                    batch.sourceId(),
                    event.eventId()
            );
        }
    }

    private record StoredEvent(
            String mode,
            String payloadJson
    ) {
    }
}