package com.seshat.logs.service;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.ElasticsearchException;
import co.elastic.clients.elasticsearch._types.FieldValue;
import co.elastic.clients.elasticsearch._types.aggregations.CompositeAggregationSource;
import co.elastic.clients.util.NamedValue;
import co.elastic.clients.elasticsearch._types.SortOrder;
import co.elastic.clients.elasticsearch._types.query_dsl.BoolQuery;
import co.elastic.clients.elasticsearch._types.query_dsl.Operator;
import co.elastic.clients.elasticsearch.core.SearchRequest;
import co.elastic.clients.elasticsearch.core.search.Hit;
import co.elastic.clients.elasticsearch.core.search.TotalHitsRelation;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.json.JsonData;
import com.seshat.ingestion.dto.ParseStatus;
import com.seshat.ingestion.dto.ParsingMode;
import com.seshat.logs.dto.LogChunkResponse;
import com.seshat.logs.dto.LogRunIds;
import com.seshat.logs.dto.LogChunkRequest;
import com.seshat.logs.dto.LogDetailResponse;
import com.seshat.logs.dto.LogSearchResponse;
import com.seshat.storage.elasticsearch.ElasticsearchBulkWriter;
import jakarta.json.JsonArray;
import jakarta.json.JsonString;
import org.springframework.stereotype.Service;
import tools.jackson.databind.node.ObjectNode;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static com.seshat.storage.elasticsearch.ElasticsearchDocumentFactory.PREVIEW_LENGTH;
import static com.seshat.storage.elasticsearch.ElasticsearchDocumentFactory.RAW_PREVIEW;

@Service
public class LogQueryService {

    private static final int MAX_PAGE_SIZE = 100;
    private static final int MAX_RESULT_WINDOW = 10_000;
    private static final int MAX_QUERY_LENGTH = 512;
    private static final String PREVIEW_SCRIPT = """
            if (doc.containsKey('raw_preview') && doc['raw_preview'].size() != 0) {
                return doc['raw_preview'].value;
            }
            String raw = params['_source']['raw'];
            int limit = (int) params.limit;
            int scan = limit * 2 + 1;
            if (raw.length() < scan) {
                scan = raw.length();
            }
            if (raw.codePointCount(0, scan) <= limit) {
                return raw;
            }
            return raw.substring(0, raw.offsetByCodePoints(0, limit - 1)) + '\u2026';
            """;

    private static final int CHUNK_SIZE = 50;
    private static final String PIT_KEEP_ALIVE = "2m";
    private static final System.Logger LOG = System.getLogger(LogQueryService.class.getName());

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

    private static String requiredPreview(Hit<ObjectNode> hit) throws IOException {
        var preview = hit.fields().get(RAW_PREVIEW);
        if (preview != null && preview.toJson() instanceof JsonArray array
                && array.size() == 1 && array.getFirst() instanceof JsonString text) {
            return text.getString();
        }
        throw new IOException("A search hit is missing its log preview.");
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

        var request = listSearchRequest(runId, sourceId, mode, parseStatus, queryText)
                .index(ElasticsearchBulkWriter.INDEX)
                .sort(s -> s.field(f -> f.field("source_id").order(SortOrder.Asc)))
                .sort(s -> s.field(f -> f.field("event_id").order(SortOrder.Asc)))
                .from((int) offset)
                .size(size)
                .trackTotalHits(t -> t.enabled(true))
                .build();

        try {
            var response = client.search(request, ObjectNode.class);

            validateResponse(response);
            return new LogSearchResponse(runId, page, size, totalCount(response),
                    items(response.hits().hits()));
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

    public LogChunkResponse chunk(LogChunkRequest input) throws IOException {
        var request = listSearchRequest(input.runId(), input.sourceId(), input.mode(),
                input.parseStatus(), input.query());
        boolean opening = input.pitId() == null;
        if (opening && input.after() != null) {
            throw new IllegalArgumentException("스냅샷 ID가 필요합니다.");
        }

        String pitId = input.pitId();
        try {
            if (opening) {
                pitId = client.openPointInTime(p -> p.index(ElasticsearchBulkWriter.INDEX)
                        .allowPartialSearchResults(false)
                        .keepAlive(t -> t.time(PIT_KEEP_ALIVE))).id();
            }
            String searchPit = pitId;
            request.pit(p -> p.id(searchPit).keepAlive(t -> t.time(PIT_KEEP_ALIVE)))
                    .sort(s -> s.field(f -> f.field("_seq_no").order(SortOrder.Desc)))
                    .sort(s -> s.field(f -> f.field("_shard_doc").order(SortOrder.Asc)))
                    .size(CHUNK_SIZE + 1)
                    .trackTotalHits(t -> t.enabled(opening));

            if (input.after() != null) {
                var after = input.after();
                request.searchAfter(List.of(FieldValue.of(Long.parseLong(after.sequence())),
                        FieldValue.of(Long.parseLong(after.shardDoc()))));
            }

            var response = client.search(request.build(), ObjectNode.class);
            if (response.pitId() != null) {
                pitId = response.pitId();
            }
            validateResponse(response);
            var hits = response.hits().hits();
            int end = Math.min(CHUNK_SIZE, hits.size());
            Long total = opening ? totalCount(response) : null;
            var items = items(hits.subList(0, end));
            if (opening && hits.isEmpty()) {
                closeAfterFailure(pitId);
                return new LogChunkResponse(null, 0L, List.of(), null);
            }
            return new LogChunkResponse(pitId, total, items,
                    hits.size() > CHUNK_SIZE ? cursor(hits.get(end - 1)) : null);
        } catch (ElasticsearchException error) {
            if (opening && pitId == null && isMissingIndex(error)) {
                return new LogChunkResponse(null, 0L, List.of(), null);
            }
            if (opening) {
                closeAfterFailure(pitId);
            } else if (error.status() == 404
                    || "search_context_missing_exception".equals(error.error().type())
                    || error.error().rootCause().stream().anyMatch(
                        cause -> "search_context_missing_exception".equals(cause.type()))) {
                throw new LogSnapshotExpiredException();
            }
            throw error;
        } catch (IOException | RuntimeException error) {
            if (opening) {
                closeAfterFailure(pitId);
            }
            throw error;
        }
    }

    public LogRunIds runIds(String text, String after) throws IOException {
        if ((text != null && text.length() > MAX_QUERY_LENGTH)
                || (after != null && after.length() > MAX_QUERY_LENGTH)) {
            throw new IllegalArgumentException("실행 ID 검색 값이 너무 깁니다.");
        }
        int size = 25;
        var request = new SearchRequest.Builder()
                .index(ElasticsearchBulkWriter.INDEX)
                .size(0)
                .trackTotalHits(t -> t.enabled(false))
                .allowPartialSearchResults(false)
                .aggregations("runs", a -> a.composite(c -> {
                    c.size(size).sources(NamedValue.of("run",
                            CompositeAggregationSource.of(source -> source.terms(t -> t.field("run_id")))));
                    if (after != null) {
                        c.after("run", after);
                    }
                    return c;
                }));
        if (text != null && !text.isBlank()) {
            String escaped = text.replace("\\", "\\\\").replace("*", "\\*").replace("?", "\\?");
            request.query(q -> q.wildcard(w -> w.field("run_id")
                    .value("*" + escaped + "*").caseInsensitive(true)));
        }

        try {
            var response = client.search(request.build(), ObjectNode.class);
            validateResponse(response);
            var result = response.aggregations().get("runs").composite();
            var buckets = result.buckets().array();
            var ids = buckets.stream().map(bucket -> bucket.key().get("run").stringValue()).toList();
            var next = result.afterKey().get("run");
            return new LogRunIds(ids, ids.size() == size && next != null ? next.stringValue() : null);
        } catch (ElasticsearchException error) {
            if (isMissingIndex(error)) {
                return new LogRunIds(List.of(), null);
            }
            throw error;
        }
    }

    // 검색에 보이는 문서만 조회해 인덱스 새로 고침 전에 알리지 않는다.
    public String visibleRevision() throws IOException {
        try {
            var response = client.search(s -> s.index(ElasticsearchBulkWriter.INDEX)
                    .size(1).trackTotalHits(t -> t.enabled(false)).source(src -> src.fetch(false))
                    .allowPartialSearchResults(false)
                    .sort(sort -> sort.field(f -> f.field("_seq_no").order(SortOrder.Desc))),
                    ObjectNode.class);
            validateResponse(response);
            var hits = response.hits().hits();
            return hits.isEmpty() ? "empty" : hits.getFirst().id() + ":" + hits.getFirst().sort().getFirst().longValue();
        } catch (ElasticsearchException error) {
            if (isMissingIndex(error)) {
                return "empty";
            }
            throw error;
        }
    }

    public void closeSnapshot(String pitId) throws IOException {
        requireNonBlank(pitId, "pit_id");
        try {
            client.closePointInTime(p -> p.id(pitId));
        } catch (ElasticsearchException error) {
            if (error.status() != 404) {
                throw error;
            }
        }
    }

    private void closeAfterFailure(String pitId) {
        if (pitId == null) {
            return;
        }
        try {
            closeSnapshot(pitId);
        } catch (IOException | ElasticsearchException error) {
            LOG.log(System.Logger.Level.DEBUG, "로그 검색 스냅샷을 닫지 못했습니다.", error);
        }
    }

    private static LogChunkResponse.Cursor cursor(Hit<ObjectNode> hit) throws IOException {
        var sort = hit.sort();
        if (sort.size() != 2 || !sort.get(0).isLong() || !sort.get(1).isLong()) {
            throw new IOException("로그 검색 커서의 정렬 값이 올바르지 않습니다.");
        }
        return new LogChunkResponse.Cursor(Long.toString(sort.get(0).longValue()),
                Long.toString(sort.get(1).longValue()));
    }

    private static void validateResponse(SearchResponse<ObjectNode> response) throws IOException {
        if (response.timedOut() || response.shards().failed().intValue() > 0) {
            throw new IOException("The search result is incomplete.");
        }
    }

    private static long totalCount(SearchResponse<ObjectNode> response) throws IOException {
        var total = response.hits().total();
        if (total == null || total.relation() != TotalHitsRelation.Eq) {
            throw new IOException("Elasticsearch did not return an exact result count.");
        }
        return total.value();
    }

    private static List<LogSearchResponse.Item> items(List<Hit<ObjectNode>> hits) throws IOException {
        var items = new ArrayList<LogSearchResponse.Item>(hits.size());
        for (var hit : hits) {
            ObjectNode document = hit.source();
            if (document == null || hit.id() == null) {
                throw new IOException("A search hit is missing its source or document ID.");
            }
            items.add(new LogSearchResponse.Item(hit.id(),
                    requiredText(document, "run_id"), requiredText(document, "source_id"),
                    requiredText(document, "event_id"), requiredText(document, "mode"),
                    requiredText(document, "parse_status"), requiredPreview(hit)));
        }
        return List.copyOf(items);
    }

    private static SearchRequest.Builder listSearchRequest(
            String runId, String sourceId, ParsingMode mode, ParseStatus parseStatus, String queryText
    ) {
        return searchRequest(runId, sourceId, mode, parseStatus, queryText)
                .source(source -> source.filter(filter -> filter.includes(
                        "run_id", "source_id", "event_id", "mode", "parse_status"
                )))
                .scriptFields(RAW_PREVIEW, field -> field.script(script -> script
                        .source(source -> source.scriptString(PREVIEW_SCRIPT))
                        .params("limit", JsonData.of(PREVIEW_LENGTH))));
    }

    private static SearchRequest.Builder searchRequest(
            String runId, String sourceId, ParsingMode mode, ParseStatus parseStatus, String queryText
    ) {
        if (sourceId != null) {
            requireNonBlank(sourceId, "source_id");
        }
        if (queryText != null && queryText.length() > MAX_QUERY_LENGTH) {
            throw new IllegalArgumentException(
                    "q must contain at most 512 characters."
            );
        }

        var query = new BoolQuery.Builder();
        if (runId != null && !runId.isBlank()) {
            query.filter(f -> f.term(t -> t.field("run_id").value(runId)));
        }

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

        return new SearchRequest.Builder()
                .allowPartialSearchResults(false)
                .query(query.build()._toQuery());
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

            var detail = document.deepCopy();
            detail.remove(RAW_PREVIEW);
            return new LogDetailResponse(documentId, detail);
        } catch (ElasticsearchException exception) {
            if (isMissingIndex(exception)) {
                throw new LogNotFoundException();
            }

            throw exception;
        }
    }
}