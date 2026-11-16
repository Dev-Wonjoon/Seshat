package com.seshat.demo;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.ElasticsearchException;
import co.elastic.clients.elasticsearch._types.query_dsl.Query;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.seshat.queue.QueueWorkBarrier;
import com.seshat.storage.elasticsearch.ElasticsearchBulkWriter;
import org.springframework.http.HttpStatus;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Service;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.web.server.ResponseStatusException;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;

@Service
public class DemoRunCleanupService {
    private static final System.Logger LOG = System.getLogger(DemoRunCleanupService.class.getName());
    private static final String INDEX = ElasticsearchBulkWriter.INDEX;
    private static final Path WORK_ROOT = Path.of("build", "demo-runs").toAbsolutePath().normalize();
    private static final RowMapper<RunEntry> RUN_ENTRY = (rs, row) -> new RunEntry(
            rs.getString("run_id"), rs.getString("source_id"), rs.getString("scenario"),
            rs.getLong("started_at_ms"), State.valueOf(rs.getString("state")));
    private final JdbcTemplate jdbc;
    private final TransactionTemplate transactions;
    private final ElasticsearchClient elasticsearch;
    private final QueueWorkBarrier workBarrier;

    public DemoRunCleanupService(JdbcTemplate jdbc, TransactionTemplate transactions,
                                 ElasticsearchClient elasticsearch, QueueWorkBarrier workBarrier) {
        this.jdbc = jdbc;
        this.transactions = transactions;
        this.elasticsearch = elasticsearch;
        this.workBarrier = workBarrier;
    }

    void register(String runId, String sourceId, DemoScenarioController.Scenario scenario) {
        jdbc.update("""
                INSERT INTO demo_run (run_id, source_id, scenario, started_at_ms, state)
                VALUES (?, ?, ?, ?, 'RUNNING')
                """, runId, sourceId, scenario.name(), System.currentTimeMillis());
    }

    void finish(String runId, boolean succeeded) {
        jdbc.update("""
                UPDATE demo_run SET state = ? WHERE run_id = ? AND state = 'RUNNING'
                """, succeeded ? State.COMPLETED.name() : State.FAILED.name(), runId);
    }

    public List<RunEntry> history() {
        return jdbc.query("""
                SELECT run_id, source_id, scenario, started_at_ms, state
                FROM demo_run WHERE state <> 'DELETED' ORDER BY started_at_ms DESC, run_id
                """, RUN_ENTRY);
    }

    public List<PreviewEntry> preview(String runId) throws IOException {
        List<RunEntry> runs = runId == null
                ? history().stream().filter(run -> run.state() != State.RUNNING).toList()
                : List.of(requireRun(runId));
        List<PreviewEntry> preview = new ArrayList<>(runs.size());
        for (RunEntry run : runs) {
            if (run.state() == State.DELETED) {
                throw new ResponseStatusException(HttpStatus.GONE, "이미 삭제된 시연입니다.");
            }
            long queueCount = jdbc.queryForObject("""
                    SELECT COUNT(*) FROM log_queue WHERE run_id = ? AND source_id = ?
                    """, Long.class, run.runId(), run.sourceId());
            preview.add(new PreviewEntry(run, queueCount, countDocuments(run)));
        }
        return preview;
    }

    public synchronized List<CleanupResult> delete(List<String> runIds) {
        if (runIds == null || runIds.isEmpty() || runIds.stream().anyMatch(id -> id == null || id.isBlank())) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "삭제할 시연 실행을 선택하세요.");
        }
        // 삭제를 시작하기 전에 요청 전체를 검증한다.
        List<RunEntry> runs = new LinkedHashSet<>(runIds).stream().map(this::requireRun).toList();
        return runs.stream().map(this::deleteRun).toList();
    }

    private RunEntry requireRun(String runId) {
        var result = jdbc.query("""
                SELECT run_id, source_id, scenario, started_at_ms, state FROM demo_run WHERE run_id = ?
                """, RUN_ENTRY, runId);
        if (result.isEmpty()) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "등록된 시연 실행을 찾을 수 없습니다.");
        }
        RunEntry run = result.getFirst();
        if (run.state() == State.RUNNING) {
            throw new ResponseStatusException(HttpStatus.CONFLICT, "실행이 종료된 뒤 삭제할 수 있습니다.");
        }
        return run;
    }

    private CleanupResult deleteRun(RunEntry run) {
        if (run.state() == State.DELETED) {
            return new CleanupResult(run.runId(), true);
        }

        try {
            workBarrier.delete(() -> transactions.executeWithoutResult(status -> deleteQueueRecords(run)));
            deleteDocuments(run);
            deleteWorkDirectory(run.runId());
            jdbc.update("UPDATE demo_run SET state = 'DELETED' WHERE run_id = ?", run.runId());
            return new CleanupResult(run.runId(), true);
        } catch (IOException | RuntimeException error) {
            LOG.log(System.Logger.Level.WARNING, "시연 데이터 삭제 실패 " + run.runId(), error);
            return new CleanupResult(run.runId(), false);
        }
    }

    private void deleteQueueRecords(RunEntry run) {
        jdbc.update("UPDATE demo_run SET state = 'DELETING' WHERE run_id = ?", run.runId());
        jdbc.update("""
                DELETE FROM json_repair_attempt WHERE job_id IN (
                    SELECT id FROM json_repair_job WHERE run_id = ? AND source_id = ?
                )
                """, run.runId(), run.sourceId());
        jdbc.update("DELETE FROM json_repair_job WHERE run_id = ? AND source_id = ?",
                run.runId(), run.sourceId());
        jdbc.update("DELETE FROM log_queue WHERE run_id = ? AND source_id = ?",
                run.runId(), run.sourceId());
    }

    private Query scope(RunEntry run) {
        return Query.of(q -> q.bool(b -> b
                .filter(f -> f.term(t -> t.field("run_id").value(run.runId())))
                .filter(f -> f.term(t -> t.field("source_id").value(run.sourceId())))));
    }

    private long countDocuments(RunEntry run) throws IOException {
        try {
            var response = elasticsearch.count(c -> c.index(INDEX).query(scope(run)));
            if (response.shards().failed().intValue() > 0) {
                throw new IOException("시연 검색 문서 건수를 모두 확인하지 못했습니다.");
            }
            return response.count();
        } catch (ElasticsearchException error) {
            if (missingIndex(error)) return 0;
            throw error;
        }
    }

    private void deleteDocuments(RunEntry run) throws IOException {
        try {
            // 마지막 Bulk 결과가 검색에 보이는 상태에서 삭제를 시작한다.
            var refreshed = elasticsearch.indices().refresh(r -> r.index(INDEX));
            if (refreshed.shards().failed().intValue() > 0) {
                throw new IOException("시연 검색 인덱스 갱신에 실패했습니다.");
            }
            var response = elasticsearch.deleteByQuery(d -> d.index(INDEX).query(scope(run)).refresh(true));
            if (Boolean.TRUE.equals(response.timedOut()) || !response.failures().isEmpty()
                    || countDocuments(run) != 0) {
                throw new IOException("시연 검색 문서가 일부 남아 있습니다.");
            }
        } catch (ElasticsearchException error) {
            if (!missingIndex(error)) throw error;
        }
    }

    private boolean missingIndex(ElasticsearchException error) {
        return error.status() == 404 && "index_not_found_exception".equals(error.error().type());
    }

    static Path workDirectory(String runId) {
        Path directory = WORK_ROOT.resolve(runId).normalize();
        if (!WORK_ROOT.equals(directory.getParent())) {
            throw new IllegalArgumentException("잘못된 시연 작업 경로입니다.");
        }
        return directory;
    }

    private void deleteWorkDirectory(String runId) throws IOException {
        Path directory = workDirectory(runId);
        try {
            Files.walkFileTree(directory, new SimpleFileVisitor<>() {
                @Override
                public FileVisitResult visitFile(Path file, BasicFileAttributes attributes) throws IOException {
                    Files.delete(file);
                    return FileVisitResult.CONTINUE;
                }

                @Override
                public FileVisitResult postVisitDirectory(Path path, IOException failure) throws IOException {
                    if (failure != null) throw failure;
                    Files.delete(path);
                    return FileVisitResult.CONTINUE;
                }
            });
        } catch (NoSuchFileException missing) {
            if (!Files.notExists(directory)) throw missing;
        }
    }

    public enum State { RUNNING, COMPLETED, FAILED, DELETING, DELETED }

    public record RunEntry(
            @JsonProperty("run_id") String runId,
            @JsonProperty("source_id") String sourceId,
            String scenario,
            @JsonProperty("started_at_ms") long startedAtMs,
            State state) {}

    public record PreviewEntry(
            RunEntry run,
            @JsonProperty("queue_count") long queueCount,
            @JsonProperty("indexed_count") long indexedCount) {}

    public record CleanupResult(@JsonProperty("run_id") String runId, boolean deleted) {}
}
