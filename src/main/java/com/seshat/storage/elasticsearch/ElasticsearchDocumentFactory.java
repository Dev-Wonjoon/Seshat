package com.seshat.storage.elasticsearch;

import com.seshat.queue.PreparedLogEvent;
import org.springframework.stereotype.Component;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.node.ObjectNode;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.List;

@Component
public class ElasticsearchDocumentFactory {

    private final JsonMapper mapper;

    public ElasticsearchDocumentFactory(JsonMapper mapper) {
        this.mapper = mapper;
    }

    public String documentId(PreparedLogEvent event) {
        byte[] identity = mapper.writeValueAsBytes(List.of(
                event.runId(),
                event.sourceId(),
                event.eventId().toString()
        ));

        try {
            byte[] hash = MessageDigest.getInstance("SHA-256").digest(identity);

            return HexFormat.of().formatHex(hash);
        } catch (NoSuchAlgorithmException exception) {
            throw new IllegalStateException(
                    "SHA-256 is unavailable.",
                    exception
            );
        }
    }

    public ObjectNode document(PreparedLogEvent event) {
        ObjectNode document = event.result().deepCopy();

        document.put("run_id", event.runId());
        document.put("source_id", event.sourceId());
        document.put("event_id", event.eventId().toString());
        document.put("mode", event.mode().name());

        return document;
    }
}
