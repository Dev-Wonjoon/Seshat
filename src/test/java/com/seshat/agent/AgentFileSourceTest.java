package com.seshat.agent;

import com.seshat.ingestion.dto.LogEventRequest;
import com.seshat.ingestion.dto.ParseStatus;
import com.seshat.ingestion.dto.ParsingMode;
import com.seshat.processing.JavaLogProcessor;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

@Timeout(10)
class AgentFileSourceTest {

    @TempDir
    Path tempDir;

    private Path inputFile;
    private AgentBuffer buffer;
    private AgentFileSource source;
    private Thread readerThread;

    @AfterEach
    void tearDown() throws InterruptedException {
        if (source != null) {
            source.close();
        }

        if (readerThread != null
                && !readerThread.join(Duration.ofSeconds(1))) {

            // 검증 실패로 큐 등록이 대기 중이어도 스레드를 정리한다.
            readerThread.interrupt();

            assertTrue(
                    readerThread.join(Duration.ofSeconds(2)),
                    "File reader did not terminate."
            );
        }
    }

    @Test
    void 줄구분자를_제외한_원문과_빈줄을_보존한다()
            throws Exception {

        String first = "  {\"message\":\"안녕\"}  ";
        String invalid = "{\"message\":";

        startSource(
                first + "\r\n"
                        + "\r\n"
                        + invalid + "\n",
                10,
                ParsingMode.CENTRAL
        );

        var firstEvent = takeAndAcknowledge(first);
        var emptyEvent = takeAndAcknowledge("");
        var invalidEvent = takeAndAcknowledge(invalid);

        assertNotNull(firstEvent.eventId());
        assertNotNull(emptyEvent.eventId());
        assertNotNull(invalidEvent.eventId());

        // CENTRAL 모드에서는 원문 파싱 결과를 만들지 않는다.
        assertNull(firstEvent.parseStatus());
        assertNull(emptyEvent.parseStatus());
        assertNull(invalidEvent.parseStatus());

        assertEquals(0, buffer.unacknowledgedCount());
    }

    @Test
    void 기존내용_수집후_추가된_줄을_수집하고_정상종료한다()
            throws Exception {

        String first = "{\"message\":\"first\"}";
        String added = "{\"message\":\"added\"}";

        startSource(
                first + "\n",
                10,
                ParsingMode.CENTRAL
        );

        takeAndAcknowledge(first);

        append(added + "\n");

        takeAndAcknowledge(added);

        // 앞서 읽은 줄을 다시 수집하지 않아야 한다.
        assertNull(
                buffer.poll(TimeUnit.MILLISECONDS.toNanos(300))
        );

        source.close();

        source.completion()
                .toCompletableFuture()
                .get(3, TimeUnit.SECONDS);

        assertTrue(
                readerThread.join(Duration.ofSeconds(3))
        );

        assertEquals(0, buffer.unacknowledgedCount());
    }

    @Test
    void 마지막_줄바꿈이_없는_조각은_줄바꿈이_추가될때_수집한다()
            throws Exception {

        String first = "{\"message\":\"first\"}";
        String fragment = "{\"message\":\"fragment\"}";

        startSource(
                first + "\n" + fragment,
                10,
                ParsingMode.CENTRAL
        );

        // 수집기가 실제로 읽기를 시작했음을 먼저 확인한다.
        takeAndAcknowledge(first);

        assertNull(
                buffer.poll(TimeUnit.MILLISECONDS.toNanos(300))
        );

        append("\n");

        takeAndAcknowledge(fragment);

        assertEquals(0, buffer.unacknowledgedCount());
    }

    @Test
    void 큐가_가득차면_중앙확인후_다음줄_수집을_계속한다()
            throws Exception {

        String first = "{\"message\":\"first\"}";
        String second = "{\"message\":\"second\"}";

        startSource(
                first + "\n" + second + "\n",
                1,
                ParsingMode.CENTRAL
        );

        var firstEntry = takeEntry();

        assertEquals(first, firstEntry.event().raw());

        // 큐에서 꺼냈어도 미확인이므로 다음 줄은 등록할 수 없다.
        assertEquals(1, buffer.unacknowledgedCount());

        assertNull(
                buffer.poll(TimeUnit.MILLISECONDS.toNanos(200))
        );

        firstEntry.acknowledge();

        // 용량 반환 후 대기 중이던 다음 줄을 수집해야 한다.
        takeAndAcknowledge(second);

        assertEquals(0, buffer.unacknowledgedCount());
    }

    @Test
    void AGENT_모드에서_파싱실패_뒤의_정상줄도_수집한다()
            throws Exception {

        String invalid = "{\"message\":";
        String valid = "{\"message\":\"next\",\"level\":\"info\"}";

        startSource(
                invalid + "\n" + valid + "\n",
                10,
                ParsingMode.AGENT
        );

        var failed = takeAndAcknowledge(invalid);

        assertEquals(ParseStatus.FAILED, failed.parseStatus());
        assertNull(failed.parsed());
        assertNotNull(failed.parseError());

        assertEquals(
                "INVALID_JSON",
                failed.parseError().code()
        );

        var successful = takeAndAcknowledge(valid);

        assertEquals(
                ParseStatus.SUCCESS,
                successful.parseStatus()
        );

        assertEquals(
                "next",
                successful.parsed()
                        .path("message")
                        .stringValue()
        );

        assertNull(successful.parseError());
        assertEquals(0, buffer.unacknowledgedCount());
    }

    @Test
    void UTF8_BOM이_있으면_이벤트를_등록하지_않고_실패를_전달한다()
            throws Exception {

        startSource(
                "\uFEFF{\"message\":\"bom\"}\n",
                10,
                ParsingMode.CENTRAL
        );

        var exception = assertThrows(
                ExecutionException.class,
                () -> source.completion()
                        .toCompletableFuture()
                        .get(3, TimeUnit.SECONDS)
        );

        var cause = assertInstanceOf(
                IllegalArgumentException.class,
                exception.getCause()
        );

        assertTrue(cause.getMessage().contains("BOM"));
        assertNull(buffer.poll(0));
        assertEquals(0, buffer.unacknowledgedCount());
    }

    @Test
    void 수집한_파일이_축소되면_실패를_전달한다()
            throws Exception {

        String raw = "{\"message\":\"before-truncate\"}";

        startSource(
                raw + "\n",
                10,
                ParsingMode.CENTRAL
        );

        takeAndAcknowledge(raw);

        Files.writeString(
                inputFile,
                "",
                StandardCharsets.UTF_8,
                StandardOpenOption.WRITE,
                StandardOpenOption.TRUNCATE_EXISTING
        );

        var exception = assertThrows(
                ExecutionException.class,
                () -> source.completion()
                        .toCompletableFuture()
                        .get(3, TimeUnit.SECONDS)
        );

        var cause = assertInstanceOf(
                IllegalStateException.class,
                exception.getCause()
        );

        assertTrue(
                cause.getMessage().contains("rotation or truncation")
        );

        assertEquals(0, buffer.unacknowledgedCount());
    }

    private void startSource(
            String content,
            int capacity,
            ParsingMode mode
    ) throws Exception {

        inputFile = tempDir.resolve("input.ndjson");

        // Files.writeString은 여기서 BOM을 추가하지 않는다.
        Files.writeString(
                inputFile,
                content,
                StandardCharsets.UTF_8
        );

        buffer = new AgentBuffer(capacity);

        var factory = new AgentEventFactory(
                new JavaLogProcessor()
        );

        source = new AgentFileSource(
                inputFile,
                buffer,
                factory,
                mode
        );

        readerThread = Thread.ofPlatform()
                .name("agent-file-source-test")
                .daemon(true)
                .unstarted(source);

        readerThread.start();
    }

    private void append(String content) throws Exception {
        Files.writeString(
                inputFile,
                content,
                StandardCharsets.UTF_8,
                StandardOpenOption.WRITE,
                StandardOpenOption.APPEND
        );
    }

    private AgentBuffer.Entry takeEntry()
            throws InterruptedException {

        var entry = buffer.poll(
                TimeUnit.SECONDS.toNanos(3)
        );

        assertNotNull(entry, "Expected a collected event.");
        return entry;
    }

    private LogEventRequest takeAndAcknowledge(
            String expectedRaw
    ) throws InterruptedException {

        var entry = takeEntry();

        assertEquals(expectedRaw, entry.event().raw());

        var event = entry.event();
        entry.acknowledge();

        return event;
    }
}