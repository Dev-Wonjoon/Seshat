package com.seshat.agent;

import com.seshat.ingestion.dto.ParsingMode;
import org.apache.commons.io.input.Tailer;
import org.apache.commons.io.input.TailerListenerAdapter;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

public final class AgentFileSource
        implements Runnable, AutoCloseable {

    private final AgentBuffer buffer;
    private final AgentEventFactory eventFactory;
    private final ParsingMode mode;
    private final Tailer tailer;

    private final AtomicBoolean started =
            new AtomicBoolean();

    private final AtomicBoolean stopRequested =
            new AtomicBoolean();

    private final AtomicReference<Throwable> failure =
            new AtomicReference<>();

    private final CompletableFuture<Void> completion =
            new CompletableFuture<>();

    public AgentFileSource(
            Path inputFile,
            AgentBuffer buffer,
            AgentEventFactory eventFactory,
            ParsingMode mode
    ) {
        Objects.requireNonNull(inputFile, "inputFile");

        this.buffer = Objects.requireNonNull(buffer);
        this.eventFactory = Objects.requireNonNull(eventFactory);
        this.mode = Objects.requireNonNull(mode);

        Path path = inputFile.toAbsolutePath().normalize();

        if (!Files.isRegularFile(path) || !Files.isReadable(path)) {
            throw new IllegalArgumentException(
                    "Input must be a readable regular file: " + path
            );
        }

        tailer = Tailer.builder()
                .setPath(path)
                .setCharset(StandardCharsets.UTF_8)
                .setTailFromEnd(false)
                .setDelayDuration(Duration.ofMillis(100))
                .setIgnoreTouch(true)
                .setReOpen(false)
                .setStartThread(false)
                .setTailerListener(new Listener())
                .get();
    }

    /**
     * 전용 수집 스레드에서 한 번만 실행한다.
     */
    @Override
    public void run() {
        if (!started.compareAndSet(false, true)) {
            throw new IllegalStateException(
                    "File source can only run once."
            );
        }

        try {
            if (!stopRequested.get()) {
                tailer.run();
            }
        } catch (Throwable exception) {
            if (!(exception instanceof StopReadingException)) {
                failure.compareAndSet(null, exception);
            }

            if (exception instanceof Error error) {
                throw error;
            }
        } finally {
            tailer.close();

            if (!stopRequested.get() && failure.get() == null) {
                failure.compareAndSet(
                        null,
                        new IllegalStateException(
                                "File source stopped unexpectedly."
                        )
                );
            }

            Throwable cause = failure.get();

            if (cause == null) {
                completion.complete(null);
            } else {
                completion.completeExceptionally(cause);
            }
        }
    }

    /**
     * AgentMain에서 정상 종료와 수집 실패를 관측한다.
     */
    public CompletionStage<Void> completion() {
        return completion.minimalCompletionStage();
    }

    /**
     * 새 줄 수집의 중단을 요청한다.
     * 진행 중인 큐 등록을 강제로 interrupt하지 않는다.
     */
    @Override
    public void close() {
        stopRequested.set(true);
        tailer.close();
    }

    private void fail(Throwable cause) {
        failure.compareAndSet(null, cause);
        tailer.close();
    }

    /**
     * 현재 Tailer 읽기 루프를 빠져나오기 위한 내부 예외.
     * 실제 실패 원인은 failure에 별도로 보관한다.
     */
    private static final class StopReadingException
            extends RuntimeException {

        private StopReadingException() {
            super(null, null, false, false);
        }
    }

    private final class Listener extends TailerListenerAdapter {

        private boolean firstLine = true;

        @Override
        public void handle(String line) {
            if (stopRequested.get() || failure.get() != null) {
                throw new StopReadingException();
            }

            if (firstLine) {
                firstLine = false;

                if (line.startsWith("\uFEFF")) {
                    fail(new IllegalArgumentException(
                            "UTF-8 BOM is unsupported."
                    ));
                    throw new StopReadingException();
                }
            }

            try {
                // trim하거나 JSON을 다시 직렬화하지 않는다.
                buffer.submit(
                        () -> eventFactory.create(line, mode)
                );
            } catch (InterruptedException exception) {
                Thread.currentThread().interrupt();

                // 현재 줄의 등록을 완료하지 못했으므로 실패로 보고한다.
                fail(exception);
                throw new StopReadingException();
            } catch (RuntimeException exception) {
                fail(exception);
                throw new StopReadingException();
            }
        }

        @Override
        public void handle(Exception exception) {
            if (exception instanceof StopReadingException) {
                return;
            }

            fail(exception);
            throw new StopReadingException();
        }

        @Override
        public void fileNotFound() {
            fail(new IllegalStateException(
                    "Input file was not found."
            ));
            throw new StopReadingException();
        }

        @Override
        public void fileRotated() {
            fail(new IllegalStateException(
                    "File rotation or truncation was detected."
            ));
            throw new StopReadingException();
        }
    }
}