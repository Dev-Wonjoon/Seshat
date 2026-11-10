package com.seshat.queue;

import com.seshat.storage.elasticsearch.BulkItemResult;
import com.seshat.storage.elasticsearch.ElasticsearchDocumentFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.Objects;
import java.util.Set;

@Service
public class QueueStorageResultService {

    private static final Set<String> DOCUMENT_ERROR_TYPES = Set.of(
            "document_parsing_exception",
            "mapper_parsing_exception",
            "strict_dynamic_mapping_exception"
    );

    private static final String UPDATE_SQL = """
        UPDATE log_queue
        SET state = ?,
            processing_started_at_ms = NULL,
            next_attempt_at_ms = ?,
            last_storage_error = ?
        WHERE id = ?
            AND run_id = ?
            AND source_id = ?
            AND event_id = ?
            AND state = 'PROCESSING'
            AND attempt_count = ?
    """;

    private final JdbcTemplate jdbcTemplate;
    private final ElasticsearchDocumentFactory documents;

    public QueueStorageResultService(
            JdbcTemplate jdbcTemplate,
            ElasticsearchDocumentFactory documents
    ) {
        this.jdbcTemplate = jdbcTemplate;
        this.documents = documents;
    }

    @Transactional
    public boolean apply(
            PreparedLogEvent event,
            BulkItemResult result
    ) {
        validateEvent(event);
        Objects.requireNonNull(result, "result");

        if(event.queueId() != result.queueId() || !documents.documentId(event).equals(result.documentId())) {
            throw new IllegalArgumentException("The storage result does not match the event.");
        }

        if(result.successful()) {
            if(result.status() < 200
                    || result.status() >= 300
                    || result.errorType() != null
                    || result.errorReason() != null) {
                throw new IllegalArgumentException("The successful result contains an invalid status or error.");
            }
            return update(event, "COMPLETED", 0, null);
        }

        String error = "%d / %s: %s".formatted(
                result.status(),
                Objects.requireNonNullElse(result.errorType(), "unknown"),
                Objects.requireNonNullElse(result.errorReason(), "No error reason was provided.")
        );

        if(isDocumentError(result)) {
            return update(event, "STORAGE_FAILED", 0, error);
        }

        return update(event, "RETRY_WAIT", nextAttemptAt(event.attemptCount()), error);
    }

    @Transactional
    public boolean retryAfterFailure(
            PreparedLogEvent event,
            String errorMessage
    ) {
        validateEvent(event);
        Objects.requireNonNull(errorMessage, "errorMessage");

        return update(event, "RETRY_WAIT", nextAttemptAt(event.attemptCount()), errorMessage);
    }

    private boolean isDocumentError(BulkItemResult result) {
        return result.status() == 400
                && result.errorType() != null
                && DOCUMENT_ERROR_TYPES.contains(result.errorType());
    }

    private long nextAttemptAt(int attemptCount) {
        int exponent = Math.min(attemptCount - 1, 6);
        long delayMillis = Math.min(
                60_000L,
                1_000L << exponent
        );

        return System.currentTimeMillis() + delayMillis;
    }

    private boolean update(PreparedLogEvent event, String state, long nextAttempt, String error) {
        int updated = jdbcTemplate.update(
                UPDATE_SQL,
                state,
                nextAttempt,
                error,
                event.queueId(),
                event.runId(),
                event.sourceId(),
                event.eventId().toString(),
                event.attemptCount()
        );

        return updated == 1;
    }

    private void validateEvent(PreparedLogEvent event) {
        Objects.requireNonNull(event, "event");

        if(event.attemptCount() < 1) {
            throw new IllegalArgumentException("The Processing attempt count must be positive");
        }
    }
}
