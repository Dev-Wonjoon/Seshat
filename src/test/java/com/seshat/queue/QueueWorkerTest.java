package com.seshat.queue;

import com.seshat.ingestion.dto.ParsingMode;
import com.seshat.storage.elasticsearch.BulkItemResult;
import com.seshat.storage.elasticsearch.ElasticsearchBulkWriter;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.json.JsonMapper;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
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
        when(processing.prepareNext()).thenReturn(Optional.empty());

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

        when(processing.prepareNext()).thenReturn(
                Optional.of(first),
                Optional.of(second),
                Optional.empty()
        );
        when(writer.write(batch)).thenReturn(List.of(success, rejected));
        when(storageResults.apply(first, success)).thenReturn(true);
        when(storageResults.apply(second, rejected)).thenReturn(true);

        assertEquals(2, worker.runOnce());

        verify(writer).write(batch);
        verify(storageResults).apply(first, success);
        verify(storageResults).apply(second, rejected);
        verifyNoMoreInteractions(storageResults);
    }

    @Test
    void 준비_중_오류가_나면_이미_준비한_이벤트를_재시도한다() {
        var first = event(1);
        var cause = new IllegalStateException("Preparation failed.");

        when(processing.prepareNext())
                .thenReturn(Optional.of(first))
                .thenThrow(cause);

        when(storageResults.retryAfterFailure(
                eq(first), anyString()
        )).thenReturn(true);

        var thrown = assertThrows(
                IllegalStateException.class,
                worker::runOnce
        );

        assertSame(cause, thrown.getCause());

        verify(storageResults).retryAfterFailure(
                eq(first),
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

        when(processing.prepareNext()).thenReturn(
                Optional.of(first),
                Optional.of(second),
                Optional.empty()
        );
        when(writer.write(batch)).thenThrow(cause);
        when(storageResults.retryAfterFailure(
                any(PreparedLogEvent.class), anyString()
        )).thenReturn(true);

        var thrown = assertThrows(
                IllegalStateException.class,
                worker::runOnce
        );

        assertSame(cause, thrown.getCause());

        verify(storageResults).retryAfterFailure(
                eq(first),
                contains("Connection refused.")
        );
        verify(storageResults).retryAfterFailure(
                eq(second),
                contains("Connection refused.")
        );
        verifyNoMoreInteractions(storageResults);
    }

    @Test
    void Bulk의_RuntimeException도_재시도_대상으로_처리한다()
            throws IOException {
        var event = event(1);
        var cause = new IllegalStateException("Bulk request rejected.");

        when(processing.prepareNext()).thenReturn(
                Optional.of(event),
                Optional.empty()
        );
        when(writer.write(List.of(event))).thenThrow(cause);
        when(storageResults.retryAfterFailure(
                eq(event), anyString()
        )).thenReturn(true);

        var thrown = assertThrows(
                IllegalStateException.class,
                worker::runOnce
        );

        assertSame(cause, thrown.getCause());

        verify(storageResults).retryAfterFailure(
                eq(event),
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
        var cause = new IllegalStateException("Database unavailable.");

        when(processing.prepareNext()).thenReturn(
                Optional.of(first),
                Optional.of(second),
                Optional.empty()
        );
        when(writer.write(List.of(first, second)))
                .thenReturn(List.of(firstResult, secondResult));

        when(storageResults.apply(first, firstResult)).thenThrow(cause);
        when(storageResults.apply(second, secondResult)).thenReturn(true);

        var thrown = assertThrows(
                IllegalStateException.class,
                worker::runOnce
        );

        assertEquals(1, thrown.getSuppressed().length);
        assertSame(cause, thrown.getSuppressed()[0].getCause());

        verify(storageResults).apply(first, firstResult);
        verify(storageResults).apply(second, secondResult);

        // DB 반영 오류를 ES 전송 실패로 처리하지 않는다.
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
        var sendFailure = new IOException("Connection refused.");
        var dbFailure = new IllegalStateException("Database unavailable.");

        when(processing.prepareNext()).thenReturn(
                Optional.of(first),
                Optional.of(second),
                Optional.empty()
        );
        when(writer.write(List.of(first, second))).thenThrow(sendFailure);

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
        assertEquals(1, thrown.getSuppressed().length);
        assertSame(dbFailure, thrown.getSuppressed()[0].getCause());

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

        when(processing.prepareNext()).thenReturn(
                Optional.of(event),
                Optional.empty()
        );
        when(writer.write(List.of(event))).thenReturn(List.of(result));
        when(storageResults.apply(event, result)).thenReturn(false);

        assertEquals(1, worker.runOnce());

        verify(storageResults).apply(event, result);
        verifyNoMoreInteractions(storageResults);
    }

    @Test
    void 한_번에_최대_50건만_준비하고_전송한다() throws IOException {
        var batch = IntStream.rangeClosed(1, 50)
                .mapToObj(this::event)
                .toList();

        var results = batch.stream()
                .map(this::success)
                .toList();

        var nextIndex = new AtomicInteger();

        when(processing.prepareNext()).thenAnswer(invocation ->
                Optional.of(batch.get(nextIndex.getAndIncrement()))
        );
        when(writer.write(batch)).thenReturn(results);
        when(storageResults.apply(
                any(PreparedLogEvent.class),
                any(BulkItemResult.class)
        )).thenReturn(true);

        assertEquals(50, worker.runOnce());

        verify(processing, times(50)).prepareNext();
        verify(writer).write(batch);

        for (int index = 0; index < batch.size(); index++) {
            verify(storageResults).apply(
                    batch.get(index),
                    results.get(index)
            );
        }

        verifyNoMoreInteractions(storageResults);
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
