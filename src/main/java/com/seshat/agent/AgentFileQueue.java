package com.seshat.agent;

import java.io.*;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.charset.StandardCharsets;
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
 * 배치 순번 + 파일 식별자 + 입력 종료 위치 + 경계 해시 + HTTP 본문 + 무결성 해시
 * <p>
 * 체크포인트:
 * 원본 파일 식별 정보 + 마지막 영속 보관 위치
 */
public final class AgentFileQueue implements AutoCloseable {

    private static final int MAGIC_V1 = 0x53455331;
    private static final int MAGIC_V2 = 0x53455332;
    private static final int MAX_FILE_ID_BYTES = 128;
    private static final int HASH_BYTES = 32;
    private static final int MAX_BODY_BYTES = 1_048_576;
    private static final int MAX_PACKET_BYTES = MAX_BODY_BYTES + 256;

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
        Path configured = inputFile.toAbsolutePath().normalize();
        this.inputFile = configured.getParent().toRealPath().resolve(configured.getFileName());
        this.checkpointFile =
                this.directory.resolve("checkpoint.properties");

        this.maxPendingBatches = maxPendingBatches;
        this.maxPendingBytes = maxPendingBytes;

        identity = Map.of(
                "input.path", this.inputFile.toString(),
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
    synchronized void append(
            AgentInputFile input,
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

        Checkpoint before = checkpoint;
        validateInput(input, before);
        var next = new Checkpoint(
                input.id(), endOffset, boundaryHash(input, endOffset),
                Math.incrementExact(before.sequence())
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

        // 역압이 풀린 뒤에도 같은 핸들에서 배치 경계를 검증한다.
        if (!before.equals(checkpoint)) {
            throw new IOException("큐 공간 대기 중 입력 체크포인트가 변경됐습니다.");
        }
        validateInput(input, before);

        if (!next.tailHash().equals(boundaryHash(input, endOffset))) {
            throw new IOException(
                    "Input changed while waiting for queue capacity."
            );
        }

        try {
            Path target = batchPath(new BatchKey(false, next.sequence()));

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

        Path path = batchPath(batch.key());

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

            if (stored.endOffset() != batch.endOffset()
                    || !stored.fileId().equals(batch.fileId())
                    || !stored.tailHash().equals(batch.tailHash())
                    || !Arrays.equals(stored.body(), batch.body())) {
                throw new IOException(
                        "Acknowledgement does not match stored batch."
                );
            }

            if (!batch.key().legacy() && checkpoint.sequence() < batch.key().order()) {
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
        if (!Files.exists(checkpointFile)) {
            if (!batches.isEmpty()) {
                throw new IOException("체크포인트 없이 배치 파일이 남아 있습니다.");
            }
            try (AgentInputFile input = AgentInputFile.open(inputFile)) {
                checkpoint = new Checkpoint(input.id(), 0, "", 0);
                saveCheckpoint(checkpoint);
            }
            return;
        }

        Properties properties = loadProperties();
        String version = properties.getProperty("queue.version", "1");
        if (!version.equals("1") && !version.equals("2")) {
            throw new IOException("지원하지 않는 체크포인트 버전입니다. " + version);
        }

        boolean legacy = version.equals("1");
        Checkpoint recovered = readCheckpoint(properties, legacy);
        long savedSequence = recovered.sequence();
        long previousSequence = 0;

        for (Path path : batches) {
            Batch batch = readBatch(path);
            pendingCount++;
            pendingBytes += Files.size(path);

            if (batch.key().legacy()) {
                if (legacy && batch.endOffset() > recovered.offset()) {
                    recovered = new Checkpoint("", batch.endOffset(), batch.tailHash(), 0);
                }
                continue;
            }

            long sequence = batch.key().order();
            if (legacy || (previousSequence != 0 && sequence - previousSequence != 1)) {
                throw new IOException("배치 순번을 복구할 수 없습니다. " + path);
            }
            previousSequence = sequence;

            // 배치 확정 뒤 체크포인트 저장 전에 종료된 경우만 복구한다.
            if (sequence > savedSequence) {
                if (sequence - savedSequence != 1
                        || !recovered.fileId().equals(batch.fileId())
                        || batch.endOffset() <= recovered.offset()) {
                    throw new IOException("미확정 배치의 입력 위치를 복구할 수 없습니다. " + path);
                }
                recovered = new Checkpoint(
                        batch.fileId(), batch.endOffset(), batch.tailHash(), sequence
                );
            }
        }

        if (legacy) {
            recovered = upgradeLegacyCheckpoint(properties, recovered);
        } else {
            try (AgentInputFile input = openCheckpointInput(recovered)) {
                validateInput(input, recovered);
            }
        }

        // 중앙 확인으로 배치가 삭제되기 전에 복구 위치를 저장한다.
        saveCheckpoint(recovered);
        checkpoint = recovered;
    }

    private Properties loadProperties() throws IOException {
        var properties = new Properties();
        try (InputStream input = Files.newInputStream(checkpointFile)) {
            properties.load(input);
        }
        for (var entry : identity.entrySet()) {
            if (!entry.getValue().equals(properties.getProperty(entry.getKey()))) {
                throw new IOException("큐 식별 정보가 일치하지 않습니다. " + entry.getKey());
            }
        }
        return properties;
    }

    private Checkpoint readCheckpoint(Properties properties, boolean legacy)
            throws IOException {
        try {
            long offset = Long.parseLong(properties.getProperty("input.offset", "-1"));
            String hash = properties.getProperty("input.tailHash", "");
            long sequence = legacy ? 0 : Long.parseLong(
                    properties.getProperty("batch.sequence", "-1")
            );
            String fileId = legacy ? "" : properties.getProperty("input.fileId");

            if (offset < 0 || sequence < 0
                    || (offset == 0 ? !hash.isEmpty() : !hash.matches("[0-9a-f]{64}"))
                    || (!legacy && !validFileId(fileId))) {
                throw new IllegalArgumentException("체크포인트 필드가 올바르지 않습니다.");
            }
            return new Checkpoint(fileId, offset, hash, sequence);
        } catch (IllegalArgumentException error) {
            throw new IOException("큐 체크포인트를 읽을 수 없습니다.", error);
        }
    }

    private Checkpoint upgradeLegacyCheckpoint(Properties old, Checkpoint position)
            throws IOException {
        try (AgentInputFile input = AgentInputFile.open(inputFile)) {
            var attrs = Files.readAttributes(inputFile, BasicFileAttributes.class);
            if (!Objects.equals(old.getProperty("input.key"), String.valueOf(attrs.fileKey()))
                    || !Objects.equals(old.getProperty("input.created"),
                            attrs.creationTime().toString())) {
                throw new IOException("기존 큐의 원본을 확인할 수 없습니다. 입력 누락 가능성이 있습니다.");
            }

            var upgraded = new Checkpoint(input.id(), position.offset(), position.tailHash(), 0);
            validateInput(input, upgraded);
            try (AgentInputFile active = AgentInputFile.open(inputFile)) {
                if (!active.id().equals(input.id())) {
                    throw new IOException("기존 큐를 승계하는 동안 활성 파일이 교체됐습니다.");
                }
            }
            return upgraded;
        } catch (NoSuchFileException missing) {
            throw missingInput(position, missing);
        }
    }

    AgentInputFile openCheckpointInput(Checkpoint value) throws IOException {
        for (Path candidate : List.of(inputFile, rotatedFile())) {
            AgentInputFile input;
            try {
                input = AgentInputFile.open(candidate);
            } catch (NoSuchFileException ignored) {
                continue;
            }

            if (value.fileId().equals(input.id())) {
                return input;
            }
            input.close();
        }
        throw missingInput(value, null);
    }

    private IOException missingInput(Checkpoint value, Throwable cause) {
        return new IOException("체크포인트의 입력 파일을 찾을 수 없습니다. 입력 누락 가능성이 있습니다. "
                + "입력=" + inputFile + ", 마지막 영속 오프셋=" + value.offset(), cause);
    }

    private void saveCheckpoint(Checkpoint value) throws IOException {
        var properties = new Properties();
        properties.putAll(identity);
        properties.setProperty("queue.version", "2");
        properties.setProperty("input.fileId", value.fileId());
        properties.setProperty("batch.sequence", Long.toString(value.sequence()));

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

    private void validateInput(AgentInputFile input, Checkpoint value) throws IOException {
        if (!value.fileId().equals(input.id()) || input.size() < value.offset()) {
            throw new IOException("입력 파일 식별자가 다르거나 파일이 잘렸습니다.");
        }
        if (value.offset() > 0
                && !value.tailHash().equals(boundaryHash(input, value.offset()))) {
            throw new IOException("입력 체크포인트 경계가 변경됐습니다.");
        }
    }

    // 줄 경계를 입력을 읽은 핸들에서 다시 확인한다.
    private String boundaryHash(AgentInputFile input, long offset) throws IOException {
        if (offset < 1) {
            throw new IOException("입력 위치는 완성 줄 뒤여야 합니다.");
        }
        int length = (int) Math.min(64, offset);
        var bytes = ByteBuffer.allocate(length);
        long start = offset - length;
        while (bytes.hasRemaining()) {
            if (input.readAt(start + bytes.position(), bytes) <= 0) {
                throw new EOFException("체크포인트 경계를 읽는 동안 입력이 잘렸습니다.");
            }
        }
        byte[] boundary = bytes.array();
        if (boundary[length - 1] != '\n') {
            throw new IOException("체크포인트 경계가 LF로 끝나지 않습니다.");
        }
        return HexFormat.of().formatHex(sha256(boundary));
    }

    private static boolean validFileId(String id) {
        return id != null && !id.isBlank()
                && id.chars().allMatch(character -> character >= 0x20 && character < 0x7f)
                && id.getBytes(StandardCharsets.UTF_8).length <= MAX_FILE_ID_BYTES;
    }

    synchronized void switchInput(AgentInputFile previous, AgentInputFile next)
            throws IOException {
        ensureOpen();
        validateInput(previous, checkpoint);
        if (previous.id().equals(next.id())) {
            throw new IOException("새 활성 파일이 이전 입력과 같습니다.");
        }

        var switched = new Checkpoint(next.id(), 0, "", checkpoint.sequence());
        try {
            saveCheckpoint(switched);
            checkpoint = switched;
        } catch (IOException error) {
            throw markFailed(error);
        }
    }

    private byte[] encode(
            Checkpoint value,
            byte[] body
    ) throws IOException {

        var payload = new ByteArrayOutputStream();

        try (var output = new DataOutputStream(payload)) {
            output.writeInt(MAGIC_V2);
            output.writeLong(value.sequence());
            output.writeUTF(value.fileId());
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
            int magic = input.readInt();
            if (magic != MAGIC_V1 && magic != MAGIC_V2) {
                throw new IOException("알 수 없는 배치 형식입니다.");
            }

            boolean legacy = magic == MAGIC_V1;
            long sequence = legacy ? 0 : input.readLong();
            String fileId = legacy ? "" : input.readUTF();
            if (!legacy && (sequence < 1 || !validFileId(fileId))) {
                throw new IOException("배치 순번 또는 파일 식별자가 올바르지 않습니다.");
            }
            long endOffset = input.readLong();
            var key = new BatchKey(legacy, legacy ? endOffset : sequence);
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

            if (!path.equals(batchPath(key))) {
                throw new IOException(
                        "Batch filename and checkpoint differ."
                );
            }

            return new Batch(key, fileId, endOffset, tailHash, body);
        }
    }

    private List<Path> batchFiles() throws IOException {
        try (var files = Files.list(directory)) {
            return files
                    .filter(path -> path.getFileName()
                            .toString()
                            .matches("(?:seq-)?[0-9]{20}\\.batch"))
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

    private Path batchPath(BatchKey key) {
        String format = key.legacy() ? "%020d.batch" : "seq-%020d.batch";
        return directory.resolve(String.format(Locale.ROOT, format, key.order()));
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

    Path rotatedFile() {
        return inputFile.resolveSibling(inputFile.getFileName() + ".1");
    }

    synchronized void validateInputFile(AgentInputFile input) throws IOException {
        ensureOpen();
        validateInput(input, checkpoint);
    }

    public record Checkpoint(String fileId, long offset, String tailHash, long sequence) {
    }

    public record BatchKey(boolean legacy, long order) {
    }

    public record Batch(
            BatchKey key, String fileId, long endOffset, String tailHash, byte[] body
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