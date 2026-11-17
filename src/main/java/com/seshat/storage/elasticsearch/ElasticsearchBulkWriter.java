package com.seshat.storage.elasticsearch;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.ElasticsearchException;
import co.elastic.clients.elasticsearch.core.BulkRequest;
import co.elastic.clients.util.BinaryData;
import com.seshat.queue.PreparedLogEvent;
import org.springframework.core.io.ClassPathResource;
import org.springframework.stereotype.Service;
import tools.jackson.databind.json.JsonMapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static com.seshat.storage.elasticsearch.ElasticsearchDocumentFactory.RAW_PREVIEW;

@Service
public class ElasticsearchBulkWriter {

    public static final String INDEX = "seshat-logs-v1";

    public static final int MAX_BATCH_SIZE = 10_000;
    private static final int MAX_BULK_REQUEST_BYTES = 32 * 1024 * 1024;
    // 고정 인덱스 이름, SHA-256 문서 ID와 NDJSON 줄바꿈을 포함한 여유분.
    private static final int ACTION_OVERHEAD_BYTES = 256;

    private final ElasticsearchClient client;
    private final ElasticsearchDocumentFactory documents;
    private final JsonMapper mapper;
    private volatile boolean mappingReady;

    public ElasticsearchBulkWriter(
            ElasticsearchClient client,
            ElasticsearchDocumentFactory documents,
            JsonMapper mapper
    ) {
        this.client = client;
        this.documents = documents;
        this.mapper = mapper;
    }

    public List<BulkItemResult> write(List<PreparedLogEvent> events) throws IOException {
        var batch = List.copyOf(events);
        if (batch.isEmpty()) {
            return List.of();
        }
        if (batch.size() > MAX_BATCH_SIZE) {
            throw new IllegalArgumentException("A bulk batch may contain at most " + MAX_BATCH_SIZE + " events.");
        }

        var requestIds = new ArrayList<String>(batch.size());
        Set<String> uniqueIds = new HashSet<>();
        for (var event : batch) {
            String documentId = documents.documentId(event);
            if (!uniqueIds.add(documentId)) {
                throw new IllegalArgumentException("Duplicate event in the bulk request.");
            }
            requestIds.add(documentId);
        }

        ensureIndexMapping();

        var results = new ArrayList<BulkItemResult>(batch.size());
        var request = new BulkRequest.Builder();
        int start = 0;
        long requestBytes = 0;
        for (int index = 0; index < batch.size(); index++) {
            byte[] json = mapper.writeValueAsBytes(documents.document(batch.get(index)));
            long operationBytes = (long) json.length + ACTION_OVERHEAD_BYTES;
            if (index > start && requestBytes + operationBytes > MAX_BULK_REQUEST_BYTES) {
                submit(request, batch.subList(start, index),
                        requestIds.subList(start, index), results);
                request = new BulkRequest.Builder();
                start = index;
                requestBytes = 0;
            }

            String documentId = requestIds.get(index);
            BinaryData document = BinaryData.of(json, "application/json");
            request.operations(operation -> operation.index(operationIndex -> operationIndex
                    .index(INDEX)
                    .id(documentId)
                    .document(document)
            ));
            requestBytes += operationBytes;
        }

        submit(request, batch.subList(start, batch.size()),
                requestIds.subList(start, batch.size()), results);
        return List.copyOf(results);
    }

    private void ensureIndexMapping() throws IOException {
        if (mappingReady) {
            return;
        }
        synchronized (this) {
            if (mappingReady) {
                return;
            }
            if (!client.indices().exists(request -> request.index(INDEX)).value()) {
                var mapping = new ClassPathResource("elasticsearch/" + INDEX + ".json");
                try (var input = mapping.getInputStream()) {
                    var created = client.indices().create(request ->
                            request.index(INDEX).withJson(input));
                    if (!created.acknowledged()) {
                        throw new IOException("Log index creation was not acknowledged.");
                    }
                    mappingReady = true;
                    return;
                } catch (ElasticsearchException exception) {
                    if (!"resource_already_exists_exception".equals(exception.error().type())) {
                        throw exception;
                    }
                }
            }
            var updated = client.indices().putMapping(request -> request
                    .index(INDEX)
                    .properties(RAW_PREVIEW, property -> property.keyword(keyword -> keyword.index(false))));
            if (!updated.acknowledged()) {
                throw new IOException("Log preview mapping update was not acknowledged.");
            }
            mappingReady = true;
        }
    }

    private void submit(
            BulkRequest.Builder request,
            List<PreparedLogEvent> batch,
            List<String> requestIds,
            List<BulkItemResult> results
    ) throws IOException {
        var response = client.bulk(request.build());

        if(response.items().size() != batch.size()) {
            throw new IOException("Bulk response item count does not match the request.");
        }


        for(int index = 0; index < batch.size(); index++) {
            var event = batch.get(index);
            var item = response.items().get(index);
            String expectedId = requestIds.get(index);

            if(!expectedId.equals(item.id()) || !INDEX.equals(item.index())) {
                throw new IOException("Bulk response item does not match the request.");
            }

            var error = item.error();

            boolean successful = error == null
                    && item.status() >= 200
                    && item.status() < 300;

            results.add(new BulkItemResult(
                    event.queueId(),
                    expectedId,
                    successful,
                    item.status(),
                    error == null ? null : error.type(),
                    error == null ? null : error.reason()
            ));
        }

    }
}
