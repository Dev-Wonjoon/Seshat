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
    public static final String RAW_PREVIEW = "raw_preview";
    public static final int PREVIEW_LENGTH = 512;

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
        var raw = document.get("raw");
        if (raw != null && raw.isString()) {
            document.put(RAW_PREVIEW, preview(raw.stringValue()));
        }

        return document;
    }

    private static String preview(String raw) {
        int limit = Math.min(raw.length(), PREVIEW_LENGTH * 2 + 1);
        if (raw.codePointCount(0, limit) <= PREVIEW_LENGTH) {
            return raw;
        }
        int end = raw.offsetByCodePoints(0, PREVIEW_LENGTH - 1);
        return raw.substring(0, end) + "\u2026";
    }
}
