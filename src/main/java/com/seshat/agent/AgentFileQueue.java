package com.seshat.agent;

import java.io.*;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;

import static java.nio.file.StandardOpenOption.*;

/**
 * 단일 수집 스레드와 단일 전송 스레드에서 사용하는 파일 큐.
 * <p>
 * 배치 파일:
 * 입력 종료 위치 + 경계 해시 + HTTP 본문 + 무결성 해시
 * <p>
 * 체크포인트:
 * 원본 파일 식별 정보 + 마지막 영속 보관 위치
 */
public final class AgentFileQueue implements AutoCloseable {

    private static final int MAGIC = 0x53455331; // 파일 형식 식별자: SES1
    private static final int HASH_BYTES = 32;
    private static final int MAX_BODY_BYTES = 1_048_576;
    private static final int MAX_PACKET_BYTES = MAX_BODY_BYTES + 128;

    private final Path directory;
    private final Path inputFile;
    private final Path checkpointFile;

    private final int maxPendingBatches;
    private final long maxPendingBytes;

    private final Map<String, String> identity;
    private final FileChannel lockChannel;

    private FileLock directoryLock;
    private Checkpoint checkpoint;
    private int pendingCount;
    private long pendingBytes;
    private boolean closed;
    private boolean failed;

    public AgentFileQueue(
            Path directory,
            Path inputFile,
            String runId,
            String sourceId,
            URI endpoint,
            int maxPendingBatches,
            long maxPendingBytes
    ) throws IOException {

        if (maxPendingBatches < 1 || maxPendingBytes < 1) {
            throw new IllegalArgumentException(
                    "Queue limits must be positive."
            );
        }

        if (runId == null || runId.isBlank()
                || sourceId == null || sourceId.isBlank()) {
            throw new IllegalArgumentException(
                    "runId and sourceId must not be blank."
            );
        }

        this.directory = directory.toAbsolutePath().normalize();
        this.inputFile = inputFile.toRealPath();
        this.checkpointFile =
                this.directory.resolve("checkpoint.properties");

        this.maxPendingBatches = maxPendingBatches;
        this.maxPendingBytes = maxPendingBytes;

        var attributes = Files.readAttributes(
                this.inputFile,
                BasicFileAttributes.class
        );

        if (!attributes.isRegularFile()
                || !Files.isReadable(this.inputFile)) {
            throw new IOException(
                    "Input must be a readable regular file."
            );
        }

        identity = Map.of(
                "input.path", this.inputFile.toString(),
                "input.key", String.valueOf(attributes.fileKey()),
                "input.created", attributes.creationTime().toString(),
                "run.id", runId,
                "source.id", sourceId,
                "endpoint", endpoint.normalize().toString()
        );

        Files.createDirectories(this.directory);

        lockChannel = FileChannel.open(
                this.directory.resolve("queue.lock"),
                CREATE,
                WRITE
        );

        try {
            directoryLock = lockChannel.tryLock();

            if (directoryLock == null) {
                throw new IOException(
                        "Queue directory is already in use."
                );
            }

            recover();

        } catch (IOException | RuntimeException error) {
            try {
                close();
            } catch (IOException closeError) {
                error.addSuppressed(closeError);
            }

            throw error;
        }
    }

    private static void atomicWrite(
            Path target,
            byte[] bytes
    ) throws IOException {

        Path temporary = target.resolveSibling(
                target.getFileName() + ".tmp"
        );

        try (FileChannel output = FileChannel.open(
                temporary,
                CREATE,
                TRUNCATE_EXISTING,
                WRITE
        )) {
            ByteBuffer buffer = ByteBuffer.wrap(bytes);

            while (buffer.hasRemaining()) {
                output.write(buffer);
            }

            output.force(true);
        }

        // 원자적 교체를 지원하지 않으면 실패로 처리한다.
        Files.move(
                temporary,
                target,
                StandardCopyOption.ATOMIC_MOVE,
                StandardCopyOption.REPLACE_EXISTING
        );
    }

    private static byte[] sha256(byte[] bytes) {
        try {
            return MessageDigest
                    .getInstance("SHA-256")
                    .digest(bytes);

        } catch (NoSuchAlgorithmException error) {
            throw new IllegalStateException(
                    "SHA-256 is unavailable.",
                    error
            );
        }
    }

    /**
     * 원본 파일에서 다음에 읽을 위치.
     * 중앙 확인 위치가 아니라 파일 큐에 영속 보관한 위치다.
     */
    public synchronized Checkpoint checkpoint() {
        return checkpoint;
    }

    public synchronized int pendingBatchCount() {
        return pendingCount;
    }

    public synchronized long pendingBytes() {
        return pendingBytes;
    }

    /**
     * 완성된 HTTP 배치와 해당 입력의 종료 위치를 함께 보관한다.
     * <p>
     * endOffset은 UTF-8 바이트 기준이며 LF 직후 위치여야 한다.
     * 용량 한도에 도달하면 중앙 확인으로 공간이 생길 때까지 대기한다.
     */
    public synchronized void append(
            long endOffset,
            byte[] httpBody
    ) throws IOException, InterruptedException {

        ensureOpen();

        if (endOffset <= checkpoint.offset()) {
            throw new IllegalArgumentException(
                    "Input checkpoint must advance."
            );
        }

        if (httpBody.length == 0
                || httpBody.length > MAX_BODY_BYTES) {
            throw new IllegalArgumentException(
                    "HTTP body must contain 1 to 1MiB bytes."
            );
        }

        // 호출자가 대기 중 원본 배열을 변경하지 못하도록 복사한다.
        byte[] body = httpBody.clone();

        validateInput(checkpoint);

        var next = new Checkpoint(
                endOffset,
                boundaryHash(endOffset)
        );

        byte[] packet = encode(next, body);

        if (packet.length > maxPendingBytes) {
            throw new IllegalArgumentException(
                    "One batch exceeds the queue byte limit."
            );
        }

        while (pendingCount >= maxPendingBatches
                || pendingBytes + packet.length > maxPendingBytes) {

            wait();
            ensureOpen();
        }

        // 대기하는 동안 파일이 교체·축소됐을 수도 있다.
        validateInput(checkpoint);

        if (!next.tailHash().equals(boundaryHash(endOffset))) {
            throw new IOException(
                    "Input changed while waiting for queue capacity."
            );
        }

        try {
            Path target = batchPath(endOffset);

            if (Files.exists(target)) {
                throw new IOException(
                        "Batch already exists: " + target
                );
            }

            // 배치 파일을 먼저 확정한다.
            atomicWrite(target, packet);

            // 여기서 종료되어도 recover()가 배치 파일의 위치를 복구한다.
            saveCheckpoint(next);

            checkpoint = next;
            pendingCount++;
            pendingBytes += packet.length;

            notifyAll();

        } catch (IOException error) {
            throw markFailed(error);
        }
    }

    /**
     * 가장 오래된 배치를 읽는다.
     * 읽었다는 이유로 파일을 삭제하지 않는다.
     * <p>
     * 단일 전송 스레드에서만 호출한다.
     */
    public synchronized Batch take()
            throws IOException, InterruptedException {

        ensureOpen();

        while (pendingCount == 0) {
            wait();
            ensureOpen();
        }

        try {
            return readBatch(oldestBatch());

        } catch (IOException error) {
            throw markFailed(error);
        }
    }

    /**
     * 중앙의 유효한 보관 확인 응답을 받은 뒤 호출한다.
     * <p>
     * checkpoint는 배치가 공개되기 전에 저장되므로,
     * 배치 파일을 지워도 원본 읽기 위치는 유지된다.
     */
    public synchronized void acknowledge(Batch batch)
            throws IOException {

        ensureOpen();

        Path path = batchPath(batch.endOffset());

        // 같은 배치에 대한 중복 확인은 아무 작업도 하지 않는다.
        if (!Files.exists(path)) {
            return;
        }

        try {
            Path oldest = oldestBatch();

            if (!oldest.equals(path)) {
                throw new IOException(
                        "Only the oldest batch can be acknowledged."
                );
            }

            Batch stored = readBatch(path);

            if (!stored.tailHash().equals(batch.tailHash())
                    || !Arrays.equals(stored.body(), batch.body())) {
                throw new IOException(
                        "Acknowledgement does not match stored batch."
                );
            }

            if (checkpoint.offset() < batch.endOffset()) {
                throw new IOException(
                        "Input checkpoint is behind the batch."
                );
            }

            long size = Files.size(path);

            Files.delete(path);

            pendingCount--;
            pendingBytes -= size;

            notifyAll();

        } catch (IOException error) {
            throw markFailed(error);
        }
    }

    private void recover() throws IOException {
        List<Path> batches = batchFiles();

        if (Files.exists(checkpointFile)) {
            checkpoint = loadCheckpoint();

        } else {
            if (!batches.isEmpty()) {
                throw new IOException(
                        "Queue contains batches but has no identity checkpoint."
                );
            }

            checkpoint = new Checkpoint(0, "");
            saveCheckpoint(checkpoint);
        }

        // 배치 확정 후 체크포인트 저장 전에 종료된 경우를 복구한다.
        for (Path path : batches) {
            Batch batch = readBatch(path);

            pendingCount++;
            pendingBytes += Files.size(path);

            if (batch.endOffset() > checkpoint.offset()) {
                checkpoint = new Checkpoint(
                        batch.endOffset(),
                        batch.tailHash()
                );
            }
        }

        validateInput(checkpoint);

        // 이후 배치를 확인·삭제해도 입력 위치가 남도록 먼저 저장한다.
        saveCheckpoint(checkpoint);
    }

    private Checkpoint loadCheckpoint() throws IOException {
        var properties = new Properties();

        try (InputStream input = Files.newInputStream(checkpointFile)) {
            properties.load(input);
        }

        for (var entry : identity.entrySet()) {
            if (!entry.getValue().equals(
                    properties.getProperty(entry.getKey())
            )) {
                throw new IOException(
                        "Queue identity differs: " + entry.getKey()
                );
            }
        }

        try {
            long offset = Long.parseLong(
                    properties.getProperty("input.offset")
            );

            String hash = properties.getProperty("input.tailHash", "");

            if (offset < 0
                    || (offset == 0 && !hash.isEmpty())
                    || (offset > 0 && !hash.matches("[0-9a-f]{64}"))) {
                throw new IllegalArgumentException(
                        "Invalid checkpoint."
                );
            }

            return new Checkpoint(offset, hash);

        } catch (IllegalArgumentException error) {
            throw new IOException(
                    "Invalid checkpoint file.",
                    error
            );
        }
    }

    private void saveCheckpoint(Checkpoint value) throws IOException {
        var properties = new Properties();
        properties.putAll(identity);

        properties.setProperty(
                "input.offset",
                Long.toString(value.offset())
        );

        properties.setProperty(
                "input.tailHash",
                value.tailHash()
        );

        var bytes = new ByteArrayOutputStream();
        properties.store(bytes, "Agent input checkpoint");

        atomicWrite(checkpointFile, bytes.toByteArray());
    }

    private void validateInput(Checkpoint value) throws IOException {
        var attributes = Files.readAttributes(
                inputFile,
                BasicFileAttributes.class
        );

        boolean sameFile =
                attributes.isRegularFile()
                        && identity.get("input.key").equals(
                        String.valueOf(attributes.fileKey())
                )
                        && identity.get("input.created").equals(
                        attributes.creationTime().toString()
                );

        if (!sameFile || attributes.size() < value.offset()) {
            throw new IOException(
                    "Input rotation or truncation detected."
            );
        }

        if (value.offset() > 0
                && !value.tailHash().equals(
                boundaryHash(value.offset())
        )) {
            throw new IOException(
                    "Input checkpoint boundary changed."
            );
        }
    }

    /**
     * 체크포인트 직전 최대 64바이트의 해시.
     * 같은 파일의 체크포인트 경계가 변경됐는지 확인한다.
     */
    private String boundaryHash(long offset) throws IOException {
        if (offset < 1) {
            throw new IOException(
                    "Checkpoint must follow a complete line."
            );
        }

        int length = (int) Math.min(64, offset);
        ByteBuffer bytes = ByteBuffer.allocate(length);

        try (FileChannel input = FileChannel.open(inputFile, READ)) {
            input.position(offset - length);

            while (bytes.hasRemaining()) {
                if (input.read(bytes) < 0) {
                    throw new EOFException(
                            "Input was truncated."
                    );
                }
            }
        }

        byte[] boundary = bytes.array();

        if (boundary[boundary.length - 1] != '\n') {
            throw new IOException(
                    "Checkpoint must immediately follow LF or CRLF."
            );
        }

        return HexFormat.of().formatHex(sha256(boundary));
    }

    private byte[] encode(
            Checkpoint value,
            byte[] body
    ) throws IOException {

        var payload = new ByteArrayOutputStream();

        try (var output = new DataOutputStream(payload)) {
            output.writeInt(MAGIC);
            output.writeLong(value.offset());
            output.writeUTF(value.tailHash());
            output.writeInt(body.length);
            output.write(body);
        }

        byte[] content = payload.toByteArray();

        var packet = new ByteArrayOutputStream(
                content.length + HASH_BYTES
        );

        packet.write(content);
        packet.write(sha256(content));

        return packet.toByteArray();
    }

    private Batch readBatch(Path path) throws IOException {
        long size = Files.size(path);

        if (size <= HASH_BYTES || size > MAX_PACKET_BYTES) {
            throw new IOException(
                    "Invalid batch file size: " + path
            );
        }

        byte[] packet = Files.readAllBytes(path);
        int contentLength = packet.length - HASH_BYTES;

        byte[] content = Arrays.copyOf(packet, contentLength);
        byte[] expectedHash = Arrays.copyOfRange(
                packet,
                contentLength,
                packet.length
        );

        if (!MessageDigest.isEqual(
                expectedHash,
                sha256(content)
        )) {
            throw new IOException(
                    "Batch checksum mismatch: " + path
            );
        }

        try (var input = new DataInputStream(
                new ByteArrayInputStream(content)
        )) {
            if (input.readInt() != MAGIC) {
                throw new IOException("Unknown batch format.");
            }

            long endOffset = input.readLong();
            String tailHash = input.readUTF();
            int bodyLength = input.readInt();

            if (endOffset < 1
                    || !tailHash.matches("[0-9a-f]{64}")
                    || bodyLength < 1
                    || bodyLength > MAX_BODY_BYTES) {
                throw new IOException("Invalid batch metadata.");
            }

            byte[] body = input.readNBytes(bodyLength);

            if (body.length != bodyLength || input.available() != 0) {
                throw new IOException("Incomplete batch body.");
            }

            if (!path.equals(batchPath(endOffset))) {
                throw new IOException(
                        "Batch filename and checkpoint differ."
                );
            }

            return new Batch(endOffset, tailHash, body);
        }
    }

    private List<Path> batchFiles() throws IOException {
        try (var files = Files.list(directory)) {
            return files
                    .filter(path -> path.getFileName()
                            .toString()
                            .matches("[0-9]{20}\\.batch"))
                    .sorted()
                    .toList();
        }
    }

    private Path oldestBatch() throws IOException {
        List<Path> files = batchFiles();

        if (files.isEmpty()) {
            throw new IOException(
                    "Queue metadata and batch files differ."
            );
        }

        return files.getFirst();
    }

    private Path batchPath(long endOffset) {
        return directory.resolve(
                String.format(Locale.ROOT, "%020d.batch", endOffset)
        );
    }

    private IOException markFailed(IOException cause) {
        failed = true;
        notifyAll();

        return new IOException(
                "File queue failed. Stored files are retained.",
                cause
        );
    }

    private void ensureOpen() throws IOException {
        if (closed || failed) {
            throw new IOException(
                    "File queue is closed or has failed."
            );
        }
    }

    @Override
    public synchronized void close() throws IOException {
        if (closed) {
            return;
        }

        closed = true;
        notifyAll();

        try {
            if (directoryLock != null) {
                directoryLock.release();
            }
        } finally {
            lockChannel.close();
        }
    }

    public Path inputFile() {
        return inputFile;
    }

    public String runId() {
        return identity.get("run.id");
    }

    public String sourceId() {
        return identity.get("source.id");
    }

    public URI endpoint() {
        return URI.create(identity.get("endpoint"));
    }

    public synchronized void validateInputFile() throws IOException {
        ensureOpen();
        validateInput(checkpoint);
    }

    public record Checkpoint(
            long offset,
            String tailHash
    ) {
    }

    public record Batch(
            long endOffset,
            String tailHash,
            byte[] body
    ) {
        public Batch {
            body = body.clone();
        }

        @Override
        public byte[] body() {
            return body.clone();
        }
    }
}