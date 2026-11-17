package com.seshat.queue;

import com.seshat.storage.elasticsearch.BulkItemResult;
import com.seshat.storage.elasticsearch.ElasticsearchDocumentFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.ArrayList;
import java.util.List;
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
        return applyBatch(List.of(event), List.of(result)).isEmpty();
    }

    @Transactional
    public List<PreparedLogEvent> applyBatch(List<PreparedLogEvent> events, List<BulkItemResult> results) {
        if (events.size() != results.size()) {
            throw new IllegalArgumentException("The number of storage results does not match the batch.");
        }
        var updates = new ArrayList<Object[]>(events.size());
        for (int index = 0; index < events.size(); index++) {
            updates.add(resultUpdate(events.get(index), results.get(index)));
        }
        return updateBatch(events, updates);
    }

    private Object[] resultUpdate(PreparedLogEvent event, BulkItemResult result) {
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
            return updateArguments(event, "COMPLETED", 0, null);
        }

        String error = "%d / %s: %s".formatted(
                result.status(),
                Objects.requireNonNullElse(result.errorType(), "unknown"),
                Objects.requireNonNullElse(result.errorReason(), "No error reason was provided.")
        );

        if(isDocumentError(result)) {
            return updateArguments(event, "STORAGE_FAILED", 0, error);
        }

        return updateArguments(event, "RETRY_WAIT", nextAttemptAt(event.attemptCount()), error);
    }

    @Transactional
    public boolean retryAfterFailure(
            PreparedLogEvent event,
            String errorMessage
    ) {
        return retryBatchAfterFailure(List.of(event), errorMessage).isEmpty();
    }

    @Transactional
    public List<PreparedLogEvent> retryBatchAfterFailure(List<PreparedLogEvent> events, String errorMessage) {
        Objects.requireNonNull(errorMessage, "errorMessage");
        var updates = new ArrayList<Object[]>(events.size());
        for (var event : events) {
            validateEvent(event);
            updates.add(updateArguments(event, "RETRY_WAIT", nextAttemptAt(event.attemptCount()), errorMessage));
        }
        return updateBatch(events, updates);
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

    private Object[] updateArguments(PreparedLogEvent event, String state, long nextAttempt, String error) {
        return new Object[]{
                state,
                nextAttempt,
                error,
                event.queueId(),
                event.runId(),
                event.sourceId(),
                event.eventId().toString(),
                event.attemptCount()
        };
    }

    private List<PreparedLogEvent> updateBatch(List<PreparedLogEvent> events, List<Object[]> updates) {
        var skipped = new ArrayList<PreparedLogEvent>();
        int[] counts = jdbcTemplate.batchUpdate(UPDATE_SQL, updates);
        for (int index = 0; index < counts.length; index++) {
            if (counts[index] != 1) {
                skipped.add(events.get(index));
            }
        }
        return skipped;
    }

    private void validateEvent(PreparedLogEvent event) {
        Objects.requireNonNull(event, "event");

        if(event.attemptCount() < 1) {
            throw new IllegalArgumentException("The Processing attempt count must be positive");
        }
    }
}
