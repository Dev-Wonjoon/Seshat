package com.seshat.queue;

import com.seshat.storage.elasticsearch.BulkItemResult;
import com.seshat.storage.elasticsearch.ElasticsearchBulkWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

@Service
public class QueueWorker {

    private static final Logger log =
            LoggerFactory.getLogger(QueueWorker.class);

    private static final int MAX_BATCH_SIZE = 50;

    private final QueueProcessingService processing;
    private final ElasticsearchBulkWriter writer;
    private final QueueStorageResultService storageResults;

    public QueueWorker(
            QueueProcessingService processing,
            ElasticsearchBulkWriter writer,
            QueueStorageResultService storageResults
    ) {
        this.processing = processing;
        this.writer = writer;
        this.storageResults = storageResults;
    }

    /**
     * 한 배치를 처리하고 ES에 전송한 이벤트 수를 반환한다.
     * 반환값은 ES 저장 성공 건수나 DB 상태 갱신 건수가 아니다.
     */
    @Transactional(propagation = Propagation.NEVER)
    public int runOnce() {
        var batch = new ArrayList<PreparedLogEvent>(MAX_BATCH_SIZE);

        try {
            for (int index = 0; index < MAX_BATCH_SIZE; index++) {
                var next = processing.prepareNext();

                if (next.isEmpty()) {
                    break;
                }

                batch.add(next.get());
            }
        } catch (RuntimeException exception) {
            var failure = new IllegalStateException(
                    "Failed to prepare the queue batch.",
                    exception
            );

            retryPrepared(
                    batch,
                    "Batch preparation failed: " + describe(exception),
                    failure
            );

            throw failure;
        }

        if (batch.isEmpty()) {
            return 0;
        }

        List<BulkItemResult> results;

        // 이 try에는 ES 전송만 포함한다.
        // 아래의 DB 결과 반영 오류가 전송 실패로 처리되지 않게 한다.
        try {
            results = writer.write(List.copyOf(batch));
        } catch (IOException | RuntimeException exception) {
            var failure = new IllegalStateException(
                    "The Elasticsearch bulk request failed.",
                    exception
            );

            retryPrepared(
                    batch,
                    "Bulk request failed: " + describe(exception),
                    failure
            );

            throw failure;
        }

        IllegalStateException updateFailure = null;

        // writer가 응답 개수와 순서별 문서 ID를 검증한다.
        for (int index = 0; index < batch.size(); index++) {
            var event = batch.get(index);

            try {
                boolean updated = storageResults.apply(
                        event,
                        results.get(index)
                );

                if (!updated) {
                    logSkipped(event);
                }
            } catch (RuntimeException exception) {
                if (updateFailure == null) {
                    updateFailure = new IllegalStateException(
                            "Failed to apply one or more bulk results to the queue."
                    );
                }

                updateFailure.addSuppressed(new IllegalStateException(
                        "Failed to apply the result for queueId="
                                + event.queueId()
                                + ", attemptCount="
                                + event.attemptCount(),
                        exception
                ));
            }
        }

        if (updateFailure != null) {
            throw updateFailure;
        }

        return batch.size();
    }

    private void retryPrepared(
            List<PreparedLogEvent> events,
            String errorMessage,
            RuntimeException failure
    ) {
        for (var event : events) {
            try {
                boolean updated = storageResults.retryAfterFailure(
                        event,
                        errorMessage
                );

                if (!updated) {
                    logSkipped(event);
                }
            } catch (RuntimeException exception) {
                // 한 이벤트의 DB 오류 때문에 뒤의 이벤트를 방치하지 않는다.
                failure.addSuppressed(new IllegalStateException(
                        "Failed to schedule a retry for queueId="
                                + event.queueId()
                                + ", attemptCount="
                                + event.attemptCount(),
                        exception
                ));
            }
        }
    }

    private void logSkipped(PreparedLogEvent event) {
        log.warn(
                "Queue update skipped: queueId={}, attemptCount={}",
                event.queueId(),
                event.attemptCount()
        );
    }

    private String describe(Exception exception) {
        String message = exception.getMessage();

        return exception.getClass().getSimpleName()
                + (message == null ? "" : ": " + message);
    }
}