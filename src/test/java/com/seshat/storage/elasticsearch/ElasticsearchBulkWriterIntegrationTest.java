package com.seshat.storage.elasticsearch;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import com.seshat.ingestion.dto.ParseStatus;
import com.seshat.ingestion.dto.ParsingMode;
import com.seshat.logs.dto.LogChunkRequest;
import com.seshat.logs.service.LogQueryService;
import com.seshat.queue.PreparedLogEvent;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.node.ObjectNode;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest(properties = {
        "spring.datasource.url=jdbc:sqlite::memory:",
        "spring.datasource.hikari.maximum-pool-size=1",
        "spring.datasource.hikari.minimum-idle=1",
        "spring.datasource.hikari.pool-name=elasticsearch-writer-test"
})
class ElasticsearchBulkWriterIntegrationTest {

    private static final String INDEX = ElasticsearchBulkWriter.INDEX;

    @Autowired
    private ElasticsearchBulkWriter writer;

    @Autowired
    private ElasticsearchDocumentFactory documents;

    @Autowired
    private ElasticsearchClient client;
    @Autowired
    private LogQueryService queries;

    @Autowired
    private JsonMapper mapper;

    @Value("${spring.elasticsearch.uris}")
    private String baseUrl;

    private final String runId = "es-test-" + UUID.randomUUID();
    private final Set<String> testDocumentIds = new HashSet<>();

    private final HttpClient http = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(5))
            .build();

    @AfterEach
    void cleanUp() throws Exception {
        try {
            for (String id : testDocumentIds) {
                var request = HttpRequest.newBuilder(documentUri(id))
                        .timeout(Duration.ofSeconds(10))
                        .DELETE()
                        .build();

                var response = http.send(
                        request,
                        HttpResponse.BodyHandlers.discarding()
                );

                assertTrue(
                        response.statusCode() == 200
                                || response.statusCode() == 404,
                        "Test document cleanup failed: "
                                + response.statusCode()
                );
            }
        } finally {
            http.close();
        }
    }

    @Test
    void 전송_식별자가_달라지면_문서_ID도_달라진다() {
        var original = prepared(1, success("{}"));

        var variants = List.of(
                new PreparedLogEvent(
                        1, runId + "-other", "source-01",
                        original.eventId(), ParsingMode.CENTRAL,
                        original.result(),
                        original.attemptCount()
                ),
                new PreparedLogEvent(
                        1, runId, "source-02",
                        original.eventId(), ParsingMode.CENTRAL,
                        original.result(),
                        original.attemptCount()
                ),
                new PreparedLogEvent(
                        1, runId, "source-01",
                        new UUID(0, 2), ParsingMode.CENTRAL,
                        original.result(),
                        original.attemptCount()
                )
        );

        for (var variant : variants) {
            assertNotEquals(
                    documents.documentId(original),
                    documents.documentId(variant)
            );
        }
    }

    @Test
    void 큐_ID가_바뀌어도_같은_이벤트는_같은_ES_문서를_갱신한다()
            throws Exception {

        var original = prepared(1, success("{\"message\":\"ok\"}"));

        var replay = new PreparedLogEvent(
                999,
                original.runId(),
                original.sourceId(),
                original.eventId(),
                original.mode(),
                original.result().deepCopy(),
                original.attemptCount()
        );

        assertEquals(
                documents.documentId(original),
                documents.documentId(replay)
        );

        var first = write(original).getFirst();
        var second = write(replay).getFirst();

        assertTrue(first.successful());
        assertTrue(second.successful());
        assertEquals(201, first.status());
        assertEquals(200, second.status());
        assertEquals(first.documentId(), second.documentId());

        // 검색 기반 count를 위해 테스트에서만 갱신을 기다린다.
        client.indices().refresh(request -> request.index(INDEX));

        long count = client.count(request -> request
                .index(INDEX)
                .query(query -> query.term(term -> term
                        .field("run_id")
                        .value(runId)
                ))
        ).count();

        assertEquals(1L, count);
    }

    @Test
    void 만_건의_bulk_응답을_요청_순서대로_반환한다() throws IOException {
        var payload = success("{}");
        var batch = IntStream.rangeClosed(1, 10_000)
                .mapToObj(sequence -> prepared(sequence, payload))
                .toList();
        try {
            var results = writer.write(batch);

            assertEquals(10_000, results.size());
            assertTrue(results.stream().allMatch(BulkItemResult::successful));
            assertEquals(
                    batch.stream().map(PreparedLogEvent::queueId).toList(),
                    results.stream().map(BulkItemResult::queueId).toList()
            );
        } finally {
            client.indices().refresh(request -> request.index(INDEX));
            client.deleteByQuery(request -> request.index(INDEX)
                    .query(query -> query.term(term -> term.field("run_id").value(runId)))
                    .refresh(true));
        }
    }

    @Test
    void 일부_문서가_거절되면_항목별_성공과_실패를_구분한다()
            throws Exception {

        var good = prepared(1, success("{\"message\":\"ok\"}"));
        var bad = prepared(2, success("{}"));

        // dynamic: strict 매핑이 거절할 필드를 추가한다.
        bad.result().put("unexpected_field", true);

        var results = write(good, bad);

        assertEquals(2, results.size());

        assertEquals(good.queueId(), results.get(0).queueId());
        assertTrue(results.get(0).successful());

        assertEquals(bad.queueId(), results.get(1).queueId());
        assertFalse(results.get(1).successful());
        assertEquals(400, results.get(1).status());
        assertNotNull(results.get(1).errorType());
        assertNotNull(results.get(1).errorReason());

        assertEquals(
                "ok",
                storedSource(good).at("/parsed/message").stringValue()
        );

        assertEquals(
                404,
                getDocument(documents.documentId(bad)).statusCode()
        );
    }

    @Test
    void 원문과_숫자_정밀도를_보존한다() throws Exception {
        BigInteger integer = new BigInteger(
                "123456789012345678901234567890"
        );
        BigDecimal decimal = new BigDecimal(
                "0.12345678901234567890123456789"
        );

        var parsed = mapper.createObjectNode();
        parsed.put("integer", integer);
        parsed.put("decimal", decimal);
        parsed.put("message", "안녕");

        String raw = "  " + mapper.writeValueAsString(parsed) + "  ";

        var result = mapper.createObjectNode();
        result.put("raw", raw);
        result.put("parse_status", "SUCCESS");
        result.set("parsed", parsed);

        var event = prepared(1, result);

        assertTrue(write(event).getFirst().successful());

        var source = storedSource(event);

        assertEquals(raw, source.path("raw").stringValue());
        assertEquals(raw, source.path("raw_preview").stringValue());
        assertEquals(runId, source.path("run_id").stringValue());
        assertEquals(
                event.eventId().toString(),
                source.path("event_id").stringValue()
        );

        assertEquals(
                integer,
                source.at("/parsed/integer")
                        .decimalValue().toBigIntegerExact()
        );
        assertEquals(
                0,
                decimal.compareTo(
                        source.at("/parsed/decimal").decimalValue()
                )
        );
        assertEquals(
                "안녕",
                source.at("/parsed/message").stringValue()
        );

        // 문서를 구성하면서 원래 처리 결과를 변경하지 않는다.
        assertFalse(result.has("run_id"));
    }

    @Test
    void 파싱_실패_이벤트도_ES에는_정상적으로_저장한다()
            throws Exception {

        String raw = "{\"message\":";

        var result = mapper.createObjectNode();
        result.put("raw", raw);
        result.put("parse_status", "FAILED");
        result.putObject("parse_error")
                .put("code", "INVALID_JSON")
                .put("message", "Invalid JSON syntax.");

        var event = prepared(1, result);
        var written = write(event).getFirst();

        assertTrue(written.successful());

        var source = storedSource(event);

        assertEquals("FAILED", source.path("parse_status").stringValue());
        assertEquals(raw, source.path("raw").stringValue());
        assertEquals(
                "INVALID_JSON",
                source.at("/parse_error/code").stringValue()
        );
        assertFalse(source.has("parsed"));
    }

    @Test
    void 대용량_기존_문서와_신규_문서를_미리보기로_조회한다() throws Exception {
        String prefix = "값" + "🌲".repeat(511);
        String suffix = " fulltextmarker";
        int rawBytes = 2 * 1024 * 1024;
        int padding = rawBytes - prefix.getBytes(StandardCharsets.UTF_8).length - suffix.length();
        String raw = prefix + "x".repeat(padding) + suffix;
        String expectedPreview = "값" + "🌲".repeat(510) + "\u2026";
        var result = mapper.createObjectNode()
                .put("raw", raw)
                .put("parse_status", "FAILED");
        var newEvents = new ArrayList<PreparedLogEvent>();
        for (int sequence = 1; sequence <= 51; sequence++) {
            var event = prepared(sequence, result);
            String id = documents.documentId(event);
            testDocumentIds.add(id);
            if (sequence <= 25) {
                var legacy = documents.document(event);
                legacy.remove("raw_preview");
                client.index(request -> request.index(INDEX).id(id).document(legacy));
            } else {
                newEvents.add(event);
            }
        }
        assertTrue(writer.write(newEvents).stream().allMatch(BulkItemResult::successful));
        client.indices().refresh(request -> request.index(INDEX));

        var first = queries.chunk(new LogChunkRequest(runId, null, null, null, null, null, null));
        String pitId = first.pitId();
        try {
            assertEquals(51L, first.totalCount());
            assertEquals(50, first.items().size());
            assertNotNull(first.nextCursor());
            for (var item : first.items()) {
                assertEquals(expectedPreview, item.rawPreview());
            }

            var next = queries.chunk(new LogChunkRequest(runId, null, null, null, null, pitId, first.nextCursor()));
            pitId = next.pitId();
            assertEquals(1, next.items().size());
            assertEquals(expectedPreview, next.items().getFirst().rawPreview());
            assertNull(next.nextCursor());
        } finally {
            queries.closeSnapshot(pitId);
        }

        var page = queries.search(runId, null, null, ParseStatus.FAILED, "fulltextmarker", 0, 51);
        assertEquals(51L, page.totalCount());
        assertEquals(51, page.items().size());
        for (var item : page.items()) {
            assertEquals(expectedPreview, item.rawPreview());
        }

        var newEvent = newEvents.getFirst();
        assertEquals(expectedPreview, storedSource(newEvent).path("raw_preview").stringValue());
        var detail = queries.detail(documents.documentId(newEvent), runId).document();
        assertEquals(raw, detail.path("raw").stringValue());
        assertFalse(detail.has("raw_preview"));
    }

    private ObjectNode success(String raw) {
        var result = mapper.createObjectNode();
        result.put("raw", raw);
        result.put("parse_status", "SUCCESS");
        result.set("parsed", mapper.readTree(raw));
        return result;
    }

    private PreparedLogEvent prepared(long sequence, ObjectNode result) {
        return new PreparedLogEvent(
                sequence,
                runId,
                "source-01",
                new UUID(0, sequence),
                ParsingMode.CENTRAL,
                result,
                1
        );
    }

    private List<BulkItemResult> write(
            PreparedLogEvent... events
    ) throws IOException {
        for (var event : events) {
            testDocumentIds.add(documents.documentId(event));
        }

        return writer.write(List.of(events));
    }

    private URI documentUri(String id) {
        String address = baseUrl.replaceAll("/+$", "");
        return URI.create(address + "/" + INDEX + "/_doc/" + id);
    }

    private HttpResponse<byte[]> getDocument(String id)
            throws Exception {
        var request = HttpRequest.newBuilder(documentUri(id))
                .timeout(Duration.ofSeconds(10))
                .GET()
                .build();

        return http.send(
                request,
                HttpResponse.BodyHandlers.ofByteArray()
        );
    }

    private JsonNode storedSource(PreparedLogEvent event)
            throws Exception {
        var response = getDocument(documents.documentId(event));

        assertEquals(200, response.statusCode());

        // 응답 JSON을 프로젝트 mapper로 읽어 숫자 정밀도를 확인한다.
        var source = mapper.readTree(response.body()).path("_source");

        assertTrue(source.isObject());
        return source;
    }
}
