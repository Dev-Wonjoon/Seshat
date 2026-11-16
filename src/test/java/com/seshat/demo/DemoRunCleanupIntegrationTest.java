package com.seshat.demo;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import com.seshat.ingestion.dto.LogBatchRequest;
import com.seshat.ingestion.dto.LogEventRequest;
import com.seshat.ingestion.dto.ParsingMode;
import com.seshat.ingestion.service.LogBatchStorageService;
import com.seshat.queue.PreparedLogEvent;
import com.seshat.storage.elasticsearch.ElasticsearchBulkWriter;
import com.seshat.storage.elasticsearch.ElasticsearchDocumentFactory;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.HttpStatus;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.server.ResponseStatusException;
import tools.jackson.databind.json.JsonMapper;

import java.nio.file.Files;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest(properties = {
        "spring.datasource.url=jdbc:sqlite::memory:",
        "spring.datasource.hikari.maximum-pool-size=1",
        "spring.datasource.hikari.minimum-idle=1",
        "spring.datasource.hikari.pool-name=demo-cleanup-test"
})
class DemoRunCleanupIntegrationTest {
    private static final String DEMO_SOURCE = "demo-source";
    private static final String INDEX = ElasticsearchBulkWriter.INDEX;

    @Autowired private DemoRunCleanupService cleanup;
    @Autowired private LogBatchStorageService storage;
    @Autowired private JdbcTemplate jdbc;
    @Autowired private ElasticsearchClient elasticsearch;
    @Autowired private ElasticsearchBulkWriter writer;
    @Autowired private ElasticsearchDocumentFactory documents;
    @Autowired private JsonMapper mapper;

    @Test
    void 삭제_미리보기의_종료된_시연만_정리하고_일반_데이터와_이후_실행을_보존한다() throws Exception {
        String completed = register();
        cleanup.finish(completed, true);
        String failed = register();
        cleanup.finish(failed, false);
        String running = register();
        String unregistered = demoShapedId();
        var selected = batch(completed, DEMO_SOURCE);
        var otherSource = batch(completed, "real-source");
        var ordinary = batch(unregistered, DEMO_SOURCE);
        var active = batch(running, DEMO_SOURCE);
        var pending = batch(failed, DEMO_SOURCE);
        var batches = List.of(selected, otherSource, ordinary, active, pending);
        batches.forEach(storage::store);

        var indexedBatches = List.of(selected, otherSource, ordinary, active);
        var indexedEvents = indexedBatches.stream().map(this::event).toList();
        var directory = DemoRunCleanupService.workDirectory(completed);
        Files.createDirectories(directory);
        Files.writeString(directory.resolve("app.log"), "test demo data");
        try {
            assertTrue(writer.write(indexedEvents).stream().allMatch(result -> result.successful()));
            elasticsearch.indices().refresh(r -> r.index(INDEX));

            assertEquals(HttpStatus.NOT_FOUND, assertThrows(ResponseStatusException.class,
                    () -> cleanup.delete(List.of(completed, unregistered))).getStatusCode());
            assertEquals(HttpStatus.CONFLICT, assertThrows(ResponseStatusException.class,
                    () -> cleanup.delete(List.of(completed, running))).getStatusCode());
            assertEquals(1, queueCount(selected));

            var preview = cleanup.preview(null);
            assertEquals(2, preview.size());
            var completedPreview = preview.stream()
                    .filter(entry -> entry.run().runId().equals(completed)).findFirst().orElseThrow();
            assertEquals(1, completedPreview.queueCount());
            assertEquals(1, completedPreview.indexedCount());

            String newer = register();
            cleanup.finish(newer, true);
            var newerBatch = batch(newer, DEMO_SOURCE);
            storage.store(newerBatch);
            var ids = preview.stream().map(entry -> entry.run().runId()).toList();
            assertTrue(cleanup.delete(ids).stream().allMatch(DemoRunCleanupService.CleanupResult::deleted));
            assertTrue(cleanup.delete(ids).stream().allMatch(DemoRunCleanupService.CleanupResult::deleted));
            assertFalse(Files.exists(directory));

            assertEquals(0, queueCount(selected));
            assertEquals(0, queueCount(pending));
            assertFalse(isIndexed(event(selected)));
            for (var preserved : List.of(otherSource, ordinary, active)) {
                assertEquals(1, queueCount(preserved));
                assertTrue(isIndexed(event(preserved)));
            }
            assertEquals(1, queueCount(newerBatch));
            assertEquals(HttpStatus.GONE, assertThrows(ResponseStatusException.class,
                    () -> storage.store(selected)).getStatusCode());
            assertEquals(1, storage.store(otherSource).duplicateCount());
            assertEquals(2, cleanup.history().size());
        } finally {
            for (var event : indexedEvents) {
                if (isIndexed(event)) {
                    elasticsearch.delete(d -> d.index(INDEX).id(documents.documentId(event)));
                }
            }
            Files.deleteIfExists(directory.resolve("app.log"));
            Files.deleteIfExists(directory);
        }
    }

    private String register() {
        String runId = demoShapedId();
        cleanup.register(runId, DEMO_SOURCE, DemoScenarioController.Scenario.CENTRAL);
        return runId;
    }

    private String demoShapedId() {
        return "demo-central-" + UUID.randomUUID();
    }

    private LogBatchRequest batch(String runId, String sourceId) {
        return new LogBatchRequest(runId, sourceId, ParsingMode.CENTRAL,
                List.of(new LogEventRequest(UUID.randomUUID(), "{}", null, null, null)));
    }

    private PreparedLogEvent event(LogBatchRequest batch) {
        var result = mapper.createObjectNode();
        result.put("raw", "{}");
        result.put("parse_status", "SUCCESS");
        result.putObject("parsed");
        return new PreparedLogEvent(1, batch.runId(), batch.sourceId(), batch.events().getFirst().eventId(),
                batch.mode(), result, 1);
    }

    private long queueCount(LogBatchRequest batch) {
        return jdbc.queryForObject("SELECT COUNT(*) FROM log_queue WHERE run_id = ? AND source_id = ?",
                Long.class, batch.runId(), batch.sourceId());
    }

    private boolean isIndexed(PreparedLogEvent event) throws Exception {
        return elasticsearch.exists(e -> e.index(INDEX).id(documents.documentId(event))).value();
    }
}
