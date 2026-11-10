package com.seshat.logs.service;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.ElasticsearchException;
import co.elastic.clients.elasticsearch._types.SortOrder;
import co.elastic.clients.elasticsearch._types.query_dsl.BoolQuery;
import co.elastic.clients.elasticsearch._types.query_dsl.Operator;
import co.elastic.clients.elasticsearch.core.SearchRequest;
import co.elastic.clients.elasticsearch.core.search.TotalHitsRelation;
import com.seshat.ingestion.dto.ParseStatus;
import com.seshat.ingestion.dto.ParsingMode;
import com.seshat.logs.dto.LogDetailResponse;
import com.seshat.logs.dto.LogSearchResponse;
import com.seshat.storage.elasticsearch.ElasticsearchBulkWriter;
import org.springframework.stereotype.Service;
import tools.jackson.databind.node.ObjectNode;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

@Service
public class LogQueryService {

    private static final int MAX_PAGE_SIZE = 100;
    private static final int MAX_RESULT_WINDOW = 10_000;
    private static final int MAX_QUERY_LENGTH = 512;
    private static final int PREVIEW_LENGTH = 512;

    private final ElasticsearchClient client;

    public LogQueryService(ElasticsearchClient client) {
        this.client = client;
    }

    private static void requireNonBlank(String value, String name) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(
                    name + " must not be blank."
            );
        }
    }

    private static String requiredText(
            ObjectNode document,
            String field
    ) throws IOException {
        var value = document.get(field);

        if (value == null || !value.isString()) {
            throw new IOException(
                    "The stored document has an invalid field: " + field
            );
        }

        return value.stringValue();
    }

    private static String preview(String raw) {
        int length = raw.codePointCount(0, raw.length());

        if (length <= PREVIEW_LENGTH) {
            return raw;
        }

        int end = raw.offsetByCodePoints(0, PREVIEW_LENGTH - 1);

        return raw.substring(0, end) + "\u2026";
    }

    private static boolean isMissingIndex(
            ElasticsearchException exception
    ) {
        return exception.status() == 404
                && "index_not_found_exception".equals(
                exception.error().type()
        );
    }

    public LogSearchResponse search(
            String runId,
            String sourceId,
            ParsingMode mode,
            ParseStatus parseStatus,
            String queryText,
            int page,
            int size
    ) throws IOException {
        requireNonBlank(runId, "run_id");

        if (sourceId != null) {
            requireNonBlank(sourceId, "source_id");
        }

        if (page < 0 || size < 1 || size > MAX_PAGE_SIZE) {
            throw new IllegalArgumentException(
                    "page must be non-negative and size must be between 1 and 100."
            );
        }

        long offset = (long) page * size;

        if (offset + size > MAX_RESULT_WINDOW) {
            throw new IllegalArgumentException(
                    "The requested page exceeds the 10000-result window."
            );
        }

        if (queryText != null && queryText.length() > MAX_QUERY_LENGTH) {
            throw new IllegalArgumentException(
                    "q must contain at most 512 characters."
            );
        }

        var query = new BoolQuery.Builder()
                .filter(f -> f.term(t -> t
                        .field("run_id")
                        .value(runId)
                ));

        if (sourceId != null) {
            query.filter(f -> f.term(t -> t
                    .field("source_id")
                    .value(sourceId)
            ));
        }

        if (mode != null) {
            query.filter(f -> f.term(t -> t
                    .field("mode")
                    .value(mode.name())
            ));
        }

        if (parseStatus != null) {
            query.filter(f -> f.term(t -> t
                    .field("parse_status")
                    .value(parseStatus.name())
            ));
        }

        if (queryText != null && !queryText.isBlank()) {
            query.must(q -> q.match(m -> m
                    .field("raw")
                    .query(queryText.strip())
                    .operator(Operator.And)
            ));
        }

        var request = new SearchRequest.Builder()
                .index(ElasticsearchBulkWriter.INDEX)
                .from((int) offset)
                .size(size)
                .trackTotalHits(t -> t.enabled(true))
                .allowPartialSearchResults(false)
                .query(query.build()._toQuery())
                .source(s -> s.filter(f -> f.includes(
                        "run_id",
                        "source_id",
                        "event_id",
                        "mode",
                        "parse_status",
                        "raw"
                )))
                .sort(s -> s.field(f -> f
                        .field("source_id")
                        .order(SortOrder.Asc)
                ))
                .sort(s -> s.field(f -> f
                        .field("event_id")
                        .order(SortOrder.Asc)
                ))
                .build();

        try {
            var response = client.search(request, ObjectNode.class);

            if (response.timedOut()
                    || response.shards().failed().intValue() > 0) {
                throw new IOException(
                        "Elasticsearch returned an incomplete search result."
                );
            }

            var total = response.hits().total();

            if (total == null || total.relation() != TotalHitsRelation.Eq) {
                throw new IOException(
                        "Elasticsearch did not return an exact result count."
                );
            }

            var items = new ArrayList<LogSearchResponse.Item>();

            for (var hit : response.hits().hits()) {
                ObjectNode document = hit.source();

                if (document == null || hit.id() == null) {
                    throw new IOException(
                            "A search hit is missing its source or document ID."
                    );
                }

                items.add(new LogSearchResponse.Item(
                        hit.id(),
                        requiredText(document, "run_id"),
                        requiredText(document, "source_id"),
                        requiredText(document, "event_id"),
                        requiredText(document, "mode"),
                        requiredText(document, "parse_status"),
                        preview(requiredText(document, "raw"))
                ));
            }

            return new LogSearchResponse(
                    runId,
                    page,
                    size,
                    total.value(),
                    List.copyOf(items)
            );
        } catch (ElasticsearchException exception) {
            if (isMissingIndex(exception)) {
                return new LogSearchResponse(
                        runId,
                        page,
                        size,
                        0,
                        List.of()
                );
            }

            throw exception;
        }
    }

    public LogDetailResponse detail(
            String documentId,
            String runId
    ) throws IOException {
        requireNonBlank(runId, "run_id");

        if (documentId == null
                || !documentId.matches("[0-9a-f]{64}")) {
            throw new IllegalArgumentException(
                    "documentId must be a lowercase SHA-256 hexadecimal string."
            );
        }

        try {
            var response = client.get(
                    g -> g
                            .index(ElasticsearchBulkWriter.INDEX)
                            .id(documentId),
                    ObjectNode.class
            );

            if (!response.found()) {
                throw new LogNotFoundException();
            }

            ObjectNode document = response.source();

            if (document == null) {
                throw new IOException(
                        "The stored document is missing its source."
                );
            }

            if (!runId.equals(requiredText(document, "run_id"))) {
                throw new LogNotFoundException();
            }

            return new LogDetailResponse(
                    documentId,
                    document.deepCopy()
            );
        } catch (ElasticsearchException exception) {
            if (isMissingIndex(exception)) {
                throw new LogNotFoundException();
            }

            throw exception;
        }
    }
}