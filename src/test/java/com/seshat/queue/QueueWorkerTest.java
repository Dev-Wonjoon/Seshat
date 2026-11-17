package com.seshat.queue;

import com.seshat.ingestion.dto.ParsingMode;
import com.seshat.storage.elasticsearch.BulkItemResult;
import com.seshat.storage.elasticsearch.ElasticsearchBulkWriter;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.json.JsonMapper;

import java.io.IOException;
import java.util.List;
import java.util.UUID;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class QueueWorkerTest {

    private final QueueProcessingService processing =
            mock(QueueProcessingService.class);

    private final ElasticsearchBulkWriter writer =
            mock(ElasticsearchBulkWriter.class);

    private final QueueStorageResultService storageResults =
            mock(QueueStorageResultService.class);

    private final QueueWorker worker =
            new QueueWorker(processing, writer, storageResults, new QueueWorkBarrier());

    private final JsonMapper mapper = JsonMapper.builder().build();

    @Test
    void 빈_큐에서는_ES를_호출하지_않는다() {
        prepared(List.of());

        assertEquals(0, worker.runOnce());

        verifyNoInteractions(writer, storageResults);
    }

    @Test
    void Bulk_일부_실패도_각_결과를_그대로_반영한다() throws IOException {
        var first = event(1);
        var second = event(2);
        var batch = List.of(first, second);

        var success = success(first);
        var rejected = new BulkItemResult(
                second.queueId(),
                documentId(second),
                false,
                429,
                "es_rejected_execution_exception",
                "Too many requests."
        );

        prepared(batch);
        when(writer.write(batch)).thenReturn(List.of(success, rejected));

        assertEquals(2, worker.runOnce());

        verify(writer).write(batch);
        verify(storageResults).applyBatch(batch, List.of(success, rejected));
        verifyNoMoreInteractions(storageResults);
    }

    @Test
    void 준비_중_오류가_나면_이미_준비한_이벤트를_재시도한다() {
        var first = event(1);
        var cause = new IllegalStateException("Preparation failed.");

        when(processing.prepareBatch(anyInt(), anyInt()))
                .thenReturn(new QueueProcessingService.PreparedBatch(List.of(first), cause));

        var thrown = assertThrows(
                IllegalStateException.class,
                worker::runOnce
        );

        assertSame(cause, thrown.getCause());

        verify(storageResults).retryBatchAfterFailure(
                eq(List.of(first)),
                contains("Batch preparation failed")
        );
        verifyNoInteractions(writer);
        verifyNoMoreInteractions(storageResults);
    }

    @Test
    void 연결_실패는_모든_준비_이벤트를_재시도한다() throws IOException {
        var first = event(1);
        var second = event(2);
        var batch = List.of(first, second);
        var cause = new IOException("Connection refused.");

        prepared(batch);
        when(writer.write(batch)).thenThrow(cause);

        var thrown = assertThrows(
                IllegalStateException.class,
                worker::runOnce
        );

        assertSame(cause, thrown.getCause());

        verify(storageResults).retryBatchAfterFailure(
                eq(batch),
                contains("Connection refused.")
        );
        verifyNoMoreInteractions(storageResults);
    }

    @Test
    void Bulk의_RuntimeException도_재시도_대상으로_처리한다()
            throws IOException {
        var event = event(1);
        var cause = new IllegalStateException("Bulk request rejected.");

        prepared(List.of(event));
        when(writer.write(List.of(event))).thenThrow(cause);

        var thrown = assertThrows(
                IllegalStateException.class,
                worker::runOnce
        );

        assertSame(cause, thrown.getCause());

        verify(storageResults).retryBatchAfterFailure(
                eq(List.of(event)),
                contains("Bulk request rejected.")
        );
        verifyNoMoreInteractions(storageResults);
    }

    @Test
    void DB_결과_반영이_실패해도_나머지_결과는_반영한다()
            throws IOException {
        var first = event(1);
        var second = event(2);
        var firstResult = success(first);
        var secondResult = success(second);
        var batch = List.of(first, second);
        var results = List.of(firstResult, secondResult);
        var cause = new IllegalStateException("Database unavailable.");

        prepared(batch);
        when(writer.write(batch)).thenReturn(results);

        when(storageResults.applyBatch(batch, results)).thenThrow(cause);
        when(storageResults.apply(first, firstResult)).thenThrow(cause);
        when(storageResults.apply(second, secondResult)).thenReturn(true);

        var thrown = assertThrows(
                IllegalStateException.class,
                worker::runOnce
        );

        assertSame(cause, thrown.getCause());
        assertEquals(1, thrown.getSuppressed().length);
        assertSame(cause, thrown.getSuppressed()[0].getCause());

        verify(storageResults).apply(first, firstResult);
        verify(storageResults).apply(second, secondResult);

        // DB 반영 오류를 ES 전송 실패로 처리하지 않는다.
        verify(storageResults, never()).retryBatchAfterFailure(anyList(), anyString());
        verify(storageResults, never()).retryAfterFailure(
                any(PreparedLogEvent.class),
                anyString()
        );
    }

    @Test
    void 재시도_등록이_실패해도_나머지_이벤트를_처리한다()
            throws IOException {
        var first = event(1);
        var second = event(2);
        var batch = List.of(first, second);
        var sendFailure = new IOException("Connection refused.");
        var batchFailure = new IllegalStateException("Batch update failed.");
        var dbFailure = new IllegalStateException("Database unavailable.");

        prepared(batch);
        when(writer.write(batch)).thenThrow(sendFailure);
        when(storageResults.retryBatchAfterFailure(eq(batch), anyString())).thenThrow(batchFailure);

        when(storageResults.retryAfterFailure(
                eq(first), anyString()
        )).thenThrow(dbFailure);

        when(storageResults.retryAfterFailure(
                eq(second), anyString()
        )).thenReturn(true);

        var thrown = assertThrows(
                IllegalStateException.class,
                worker::runOnce
        );

        assertSame(sendFailure, thrown.getCause());
        assertEquals(2, thrown.getSuppressed().length);
        assertSame(batchFailure, thrown.getSuppressed()[0]);
        assertSame(dbFailure, thrown.getSuppressed()[1].getCause());

        verify(storageResults).retryBatchAfterFailure(eq(batch), anyString());
        verify(storageResults).retryAfterFailure(
                eq(first), anyString()
        );
        verify(storageResults).retryAfterFailure(
                eq(second), anyString()
        );
        verifyNoMoreInteractions(storageResults);
    }

    @Test
    void 상태_조건이_달라_갱신하지_않았어도_재시도로_덮어쓰지_않는다()
            throws IOException {
        var event = event(1);
        var result = success(event);

        prepared(List.of(event));
        when(writer.write(List.of(event))).thenReturn(List.of(result));
        when(storageResults.applyBatch(List.of(event), List.of(result))).thenReturn(List.of(event));

        assertEquals(1, worker.runOnce());

        verify(storageResults).applyBatch(List.of(event), List.of(result));
        verifyNoMoreInteractions(storageResults);
    }

    @Test
    void 한_번에_최대_10000건을_배치로_준비하고_반영한다() throws IOException {
        var batch = IntStream.rangeClosed(1, 10_000)
                .mapToObj(this::event)
                .toList();

        var results = batch.stream()
                .map(this::success)
                .toList();

        prepared(batch);
        when(writer.write(batch)).thenReturn(results);

        assertEquals(10_000, worker.runOnce());

        verify(processing).prepareBatch(10_000, 32 * 1024 * 1024);
        verifyNoMoreInteractions(processing);
        verify(writer).write(batch);
        verify(storageResults).applyBatch(batch, results);
        verifyNoMoreInteractions(storageResults);
    }

    private void prepared(List<PreparedLogEvent> batch) {
        when(processing.prepareBatch(anyInt(), anyInt()))
                .thenReturn(new QueueProcessingService.PreparedBatch(batch, null));
    }

    private PreparedLogEvent event(int id) {
        var result = mapper.createObjectNode();
        result.put("raw", "{}");
        result.put("parse_status", "SUCCESS");
        result.set("parsed", mapper.createObjectNode());

        return new PreparedLogEvent(
                id,
                "worker-test",
                "source-01",
                new UUID(0, id),
                ParsingMode.CENTRAL,
                result,
                1
        );
    }

    private BulkItemResult success(PreparedLogEvent event) {
        return new BulkItemResult(
                event.queueId(),
                documentId(event),
                true,
                201,
                null,
                null
        );
    }

    private String documentId(PreparedLogEvent event) {
        // 실제 문서 ID 생성은 기존 테스트가 검증한다.
        return "document-" + event.queueId();
    }
}
