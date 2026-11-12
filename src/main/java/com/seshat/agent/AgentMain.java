package com.seshat.agent;

import com.seshat.control.dto.AgentControlMessages.Target;
import com.seshat.ingestion.dto.ParsingMode;
import com.seshat.processing.JavaLogProcessor;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Locale;
import java.util.UUID;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

public final class AgentMain {

    private static final long DEFAULT_MAX_QUEUE_BYTES =
            256L * 1024 * 1024;

    private static final System.Logger log =
            System.getLogger(AgentMain.class.getName());

    public static void main(String[] args) throws Exception {
        if (args.length < 7 || args.length > 8) {
            throw new IllegalArgumentException(
                    "Usage: <runId> <sourceId> <CENTRAL|AGENT> "
                            + "<inputFile> <batchEndpoint> "
                            + "<maxPendingBatches> <queueDirectory> "
                            + "[maxPendingBytes]"
            );
        }

        String runId = args[0];
        String sourceId = args[1];

        ParsingMode mode = ParsingMode.valueOf(
                args[2].toUpperCase(Locale.ROOT)
        );

        Path inputFile = Path.of(args[3]);
        URI endpoint = URI.create(args[4]);

        int maxPendingBatches = Integer.parseInt(args[5]);
        Path queueDirectory = Path.of(args[6]);

        long maxPendingBytes = args.length == 8
                ? Long.parseLong(args[7])
                : DEFAULT_MAX_QUEUE_BYTES;

        if (runId.isBlank() || sourceId.isBlank()) {
            throw new IllegalArgumentException(
                    "runId and sourceId must not be blank."
            );
        }

        String scheme = endpoint.getScheme();

        if (!endpoint.isAbsolute()
                || endpoint.getHost() == null
                || (!"http".equalsIgnoreCase(scheme)
                && !"https".equalsIgnoreCase(scheme))) {

            throw new IllegalArgumentException(
                    "Endpoint must be an absolute HTTP or HTTPS URL."
            );
        }

        try (var agent = new AgentRuntime(
                queueDirectory,
                inputFile,
                endpoint,
                runId,
                sourceId,
                mode,
                maxPendingBatches,
                maxPendingBytes
        )) {
            Thread shutdownHook = Thread.ofPlatform()
                    .name("seshat-agent-shutdown")
                    .unstarted(agent::close);

            Runtime.getRuntime().addShutdownHook(shutdownHook);

            try {
                agent.start();
                agent.awaitStop();

            } finally {
                agent.close();

                try {
                    Runtime.getRuntime()
                            .removeShutdownHook(shutdownHook);

                } catch (IllegalStateException ignored) {
                    // JVM 종료가 시작된 경우에는 제거하지 않는다.
                }
            }

            agent.throwIfFailed();
        }
    }

    private static final class AgentRuntime
            implements AutoCloseable {

        private static final Duration DRAIN_TIMEOUT =
                Duration.ofSeconds(30);

        private static final Duration STOP_TIMEOUT =
                Duration.ofSeconds(5);

        private final AgentFileQueue queue;
        private final AgentModeState modeState;
        private final DurableAgentFileSource source;
        private final DurableAgentBatchSender sender;
        private final AgentHeartbeatReporter heartbeat;
        private final AgentControlClient control;

        private final Thread readerThread;
        private final Thread senderThread;

        private final CountDownLatch stopSignal =
                new CountDownLatch(1);

        private final AtomicReference<Throwable> failure =
                new AtomicReference<>();

        private final AtomicBoolean closing =
                new AtomicBoolean();

        // start()와 close()의 synchronized 구간에서 접근한다.
        private boolean started;
        private boolean closed;

        private AgentRuntime(
                Path queueDirectory,
                Path inputFile,
                URI endpoint,
                String runId,
                String sourceId,
                ParsingMode mode,
                int maxPendingBatches,
                long maxPendingBytes
        ) throws IOException {

            modeState = new AgentModeState(mode);
            queue = new AgentFileQueue(
                    queueDirectory,
                    inputFile,
                    runId,
                    sourceId,
                    endpoint,
                    maxPendingBatches,
                    maxPendingBytes
            );

            try {
                var eventFactory = new AgentEventFactory(
                        new JavaLogProcessor()
                );

                source = new DurableAgentFileSource(
                        queue,
                        eventFactory,
                        modeState
                );

                sender = new DurableAgentBatchSender(queue);

                UUID instanceId = UUID.randomUUID();
                heartbeat = new AgentHeartbeatReporter(
                        queue,
                        modeState,
                        instanceId,
                        maxPendingBatches,
                        maxPendingBytes
                );
                control = new AgentControlClient(
                        endpoint.resolve("../agents/control/sync"),
                        new Target(runId, sourceId, instanceId),
                        modeState
                );

                readerThread = Thread.ofPlatform()
                        .name("seshat-agent-reader")
                        .daemon(true)
                        .unstarted(source);

                senderThread = Thread.ofPlatform()
                        .name("seshat-agent-sender")
                        .daemon(true)
                        .unstarted(sender);

                observe("File source", source.completion());
                observe("Batch sender", sender.completion());

            } catch (IOException | RuntimeException | Error error) {
                try {
                    queue.close();

                } catch (IOException closeError) {
                    error.addSuppressed(closeError);
                }

                throw error;
            }
        }

        private static boolean awaitThread(
                Thread thread,
                long deadline
        ) throws InterruptedException {

            if (!thread.isAlive()) {
                return true;
            }

            long remaining = deadline - System.nanoTime();

            if (remaining <= 0) {
                return !thread.isAlive();
            }

            return thread.join(Duration.ofNanos(remaining));
        }

        private void observe(
                String name,
                CompletionStage<Void> completion
        ) {
            completion.whenComplete((unused, error) -> {
                if (error != null) {
                    recordFailure(error);

                } else if (!closing.get()) {
                    recordFailure(new IllegalStateException(
                            name + " stopped unexpectedly."
                    ));
                }
            });
        }

        private synchronized void start() {
            if (closed) {
                throw new IllegalStateException(
                        "Agent is already closed."
                );
            }

            if (started) {
                throw new IllegalStateException(
                        "Agent has already started."
                );
            }

            started = true;

            try {
                // 복구된 배치가 있으면 먼저 전송할 수 있다.
                senderThread.start();
                readerThread.start();
                heartbeat.start();
                control.start();

            } catch (RuntimeException | Error error) {
                recordFailure(error);
                throw error;
            }
        }

        private void awaitStop() throws InterruptedException {
            stopSignal.await();
        }

        private void recordFailure(Throwable error) {
            if (failure.compareAndSet(null, error)) {
                log.log(
                        System.Logger.Level.ERROR,
                        "Agent failed; unacknowledged files are retained.",
                        error
                );
            }

            stopSignal.countDown();
        }

        private void throwIfFailed() {
            Throwable error = failure.get();

            if (error != null) {
                throw new IllegalStateException(
                        "Agent execution failed.",
                        error
                );
            }
        }

        private void drain() throws InterruptedException {
            long deadline = System.nanoTime()
                    + DRAIN_TIMEOUT.toNanos();

            // 수집기가 완성된 마지막 배치를 파일 큐에 보관하도록
            // 전송기는 계속 실행한 상태에서 기다린다.
            if (!awaitThread(readerThread, deadline)) {
                recordFailure(new TimeoutException(
                        "File source did not stop within the deadline."
                ));
                return;
            }

            while (failure.get() == null
                    && queue.pendingBatchCount() > 0
                    && System.nanoTime() < deadline) {

                Thread.sleep(25);
            }

            if (failure.get() == null
                    && queue.pendingBatchCount() > 0) {

                recordFailure(new TimeoutException(
                        "Unacknowledged batches remain on disk."
                ));
            }
        }

        @Override
        public synchronized void close() {
            if (closed) {
                return;
            }

            closed = true;
            closing.set(true);
            stopSignal.countDown();

            boolean interrupted = false;

            // 새 입력을 중단하고 제어와 상태 보고를 종료한다.
            source.close();
            control.close();
            heartbeat.close();

            try {
                if (started && failure.get() == null) {
                    drain();
                }

            } catch (InterruptedException error) {
                interrupted = true;
                recordFailure(error);

            } finally {
                sender.close();

                // 큐 용량 대기나 네트워크 재시도를 해제한다.
                readerThread.interrupt();
                senderThread.interrupt();

                try {
                    long deadline = System.nanoTime()
                            + STOP_TIMEOUT.toNanos();

                    boolean readerStopped =
                            awaitThread(readerThread, deadline);

                    boolean senderStopped =
                            awaitThread(senderThread, deadline);

                    if (readerStopped && senderStopped) {
                        // 두 스레드가 파일 큐 사용을 끝낸 뒤 닫는다.
                        queue.close();

                    } else {
                        recordFailure(new TimeoutException(
                                "Agent threads did not terminate. "
                                        + "Queue is not closed while in use."
                        ));
                    }

                } catch (InterruptedException error) {
                    interrupted = true;
                    recordFailure(error);

                } catch (IOException error) {
                    recordFailure(error);
                }

                log.log(
                        System.Logger.Level.INFO,
                        "Agent stopped. Remaining batches: "
                                + queue.pendingBatchCount()
                );

                if (interrupted) {
                    Thread.currentThread().interrupt();
                }
            }
        }
    }
}