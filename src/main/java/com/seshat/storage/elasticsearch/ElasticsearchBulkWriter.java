package com.seshat.storage.elasticsearch;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.BulkRequest;
import co.elastic.clients.util.BinaryData;
import com.seshat.queue.PreparedLogEvent;
import org.springframework.stereotype.Service;
import tools.jackson.databind.json.JsonMapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

@Service
public class ElasticsearchBulkWriter {

    public static final String INDEX = "seshat-logs-v1";

    private static final int MAX_BATCH_SIZE = 50;

    private final ElasticsearchClient client;
    private final ElasticsearchDocumentFactory documents;
    private final JsonMapper mapper;

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

        if(batch.isEmpty()) {
            return List.of();
        }

        if(batch.size() > MAX_BATCH_SIZE) {
            throw new IllegalArgumentException("A bulk request may contain at most 50 events.");
        }

        var request = new BulkRequest.Builder();
        var requestIds = new ArrayList<String>(batch.size());
        Set<String> uniqueIds = new HashSet<>();

        for(var event : batch) {
            String documentId = documents.documentId(event);

            if(!uniqueIds.add(documentId)) {
                throw new IllegalArgumentException("Duplicate event in the bulk request.");
            }

            byte[] json = mapper.writeValueAsBytes(documents.document(event));

            BinaryData document = BinaryData.of(json, "application/json");

            requestIds.add(documentId);

            request.operations(operation -> operation.index(index -> index
                    .index(INDEX)
                    .id(documentId)
                    .document(document)
            ));
        }

        var response = client.bulk(request.build());

        if(response.items().size() != batch.size()) {
            throw new IOException("Bulk response item count does not match the request.");
        }

        var results = new ArrayList<BulkItemResult>(batch.size());

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

        return List.copyOf(results);
    }
}
