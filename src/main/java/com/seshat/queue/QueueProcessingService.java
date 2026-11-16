package com.seshat.queue;

import com.seshat.ingestion.dto.LogEventRequest;
import com.seshat.ingestion.dto.ParseStatus;
import com.seshat.ingestion.dto.ParsingMode;
import com.seshat.processing.JavaLogProcessor;
import com.seshat.repair.JsonRepairRepository;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionTemplate;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.node.ObjectNode;

import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

@Service
public class QueueProcessingService {

    private static final String CLAIM_SQL = """
        UPDATE log_queue
        SET state = 'PROCESSING',
            processing_started_at_ms = ?,
            attempt_count = attempt_count + 1,
            next_attempt_at_ms = 0
        WHERE id = (
            SELECT id
            FROM log_queue
            WHERE state = 'PENDING'
               OR (
                   state = 'RETRY_WAIT'
                   AND next_attempt_at_ms <= ?
               )
            ORDER BY id
            LIMIT 1
        )
          AND (
              state = 'PENDING'
              OR (
                  state = 'RETRY_WAIT'
                  AND next_attempt_at_ms <= ?
              )
          )
        RETURNING id, run_id, source_id, event_id, mode,
                  payload_json, result_json, attempt_count
        """;

    private final JdbcTemplate jdbcTemplate;
    private final JsonMapper mapper;
    private final JavaLogProcessor processor;
    private final TransactionTemplate transactions;
    private final JsonRepairRepository repairJobs;
    private final boolean repairEnabled;

    public QueueProcessingService(
            JdbcTemplate jdbcTemplate,
            JsonMapper mapper,
            JavaLogProcessor processor,
            PlatformTransactionManager transactionManager,
            JsonRepairRepository repairJobs,
            @Value("${seshat.repair.enabled:false}") boolean repairEnabled
    ) {
        this.jdbcTemplate = jdbcTemplate;
        this.mapper = mapper;
        this.processor = processor;
        this.transactions = new TransactionTemplate(transactionManager);
        this.repairJobs = repairJobs;
        this.repairEnabled = repairEnabled;
    }

    @Transactional(propagation = Propagation.NEVER)
    public Optional<PreparedLogEvent> prepareNext() {
        var next = claimNext();

        if(next.isEmpty()) {
            return Optional.empty();
        }

        var queued = next.get();

        try {
            ObjectNode result = prepareResult(queued);
            String resultJson = mapper.writeValueAsString(result);

            saveResult(queued, result, resultJson);

            return Optional.of(new PreparedLogEvent(
                    queued.id(),
                    queued.runId(),
                    queued.sourceId(),
                    queued.eventId(),
                    queued.mode(),
                    result,
                    queued.attemptCount()
            ));
        } catch (RuntimeException exception) {
            try {
                release(queued);
            } catch (RuntimeException runtimeException) {
                exception.addSuppressed(runtimeException);
            }

            throw exception;
        }
    }

    private Optional<ClaimedEvent> claimNext() {
        var events = transactions.execute(status -> {
            long now = System.currentTimeMillis();

            return jdbcTemplate.query(
                    CLAIM_SQL,
                    (resultSet, rowNum) -> new ClaimedEvent(
                            resultSet.getLong("id"),
                            resultSet.getString("run_id"),
                            resultSet.getString("source_id"),
                            UUID.fromString(resultSet.getString("event_id")),
                            ParsingMode.valueOf(resultSet.getString("mode")),
                            resultSet.getString("payload_json"),
                            resultSet.getString("result_json"),
                            resultSet.getInt("attempt_count")
                    ),
                    now,
                    now,
                    now
            );
        });

        return Objects.requireNonNull(events).stream().findFirst();
    }

    private ObjectNode prepareResult(ClaimedEvent queued) {
        // 이미 결과가 보관되어 있다면 재파싱하지 않는다.
        if (queued.resultJson() != null) {
            var cached = mapper.readTree(queued.resultJson());

            if (cached instanceof ObjectNode object) {
                return object;
            }

            throw new IllegalStateException(
                    "Stored processing result must be a JSON object."
            );
        }

        var event = mapper.readValue(
                queued.payloadJson(),
                LogEventRequest.class
        );

        if (queued.mode() == ParsingMode.CENTRAL) {
            return event.logFormat() == null
                    ? processor.process(event.raw())
                    : processor.process(event.raw(), event.logFormat());
        }

        ObjectNode result = mapper.createObjectNode();
        result.put("raw", event.raw());
        result.put("parse_status", event.parseStatus().name());

        if (event.parseStatus() == ParseStatus.SUCCESS) {
            result.set("parsed", event.parsed().deepCopy());
        } else {
            ObjectNode error = mapper.createObjectNode();
            error.put("code", event.parseError().code());
            error.put("message", event.parseError().message());

            result.set("parse_error", error);
        }

        return result;
    }

    private void saveResult(
            ClaimedEvent queued,
            ObjectNode result,
            String resultJson
    ) {
        transactions.executeWithoutResult(status -> {
            int updated = jdbcTemplate.update(
                    """
                    UPDATE log_queue
                    SET result_json = ?
                    WHERE id = ?
                      AND state = 'PROCESSING'
                      AND attempt_count = ?
                    """,
                    resultJson,
                    queued.id(),
                    queued.attemptCount()
            );

            if (updated != 1) {
                throw new IllegalStateException(
                        "The claimed event could not be updated."
                );
            }

            if (repairEnabled
                    && ParseStatus.FAILED.name().equals(result.path("parse_status").stringValue())
                    && "INVALID_JSON".equals(result.at("/parse_error/code").stringValue())) {
                repairJobs.register(queued.runId(), queued.sourceId(), queued.eventId().toString());
            }
        });
    }

    private void release(ClaimedEvent queued) {
        transactions.executeWithoutResult(status ->
                jdbcTemplate.update(
                        """
                        UPDATE log_queue
                        SET state = 'PENDING',
                            processing_started_at_ms = NULL
                        WHERE id = ?
                          AND state = 'PROCESSING'
                          AND attempt_count = ?
                        """,
                        queued.id(),
                        queued.attemptCount()
                )
        );
    }

    private record ClaimedEvent(
            long id,
            String runId,
            String sourceId,
            UUID eventId,
            ParsingMode mode,
            String payloadJson,
            String resultJson,
            int attemptCount
    ) {}
}
