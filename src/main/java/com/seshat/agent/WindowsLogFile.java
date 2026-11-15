package com.seshat.agent;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.MemoryLayout;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.SymbolLookup;
import java.lang.invoke.MethodHandle;
import java.nio.ByteBuffer;
import java.nio.file.AccessDeniedException;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.HexFormat;

import static java.lang.foreign.MemorySegment.NULL;
import static java.lang.foreign.ValueLayout.*;
import static java.nio.charset.StandardCharsets.UTF_16LE;

final class WindowsLogFile implements AgentInputFile {
    private static final int GENERIC_READ = 0x80000000;
    private static final int FILE_SHARE_READ = 1;
    private static final int FILE_SHARE_WRITE = 2;
    private static final int FILE_SHARE_DELETE = 4;
    private static final int OPEN_EXISTING = 3;
    private static final int FILE_ATTRIBUTE_NORMAL = 0x80;
    private static final int FILE_ID_INFO = 18;
    private static final int FILE_ID_INFO_BYTES = 24;
    private static final int FILE_BEGIN = 0;
    private static final int ERROR_HANDLE_EOF = 38;

    private static final Linker LINKER = Linker.nativeLinker();
    private static final SymbolLookup KERNEL32 =
            SymbolLookup.libraryLookup("Kernel32.dll", Arena.global());
    private static final MemoryLayout CALL_STATE = Linker.Option.captureStateLayout();
    private static final long LAST_ERROR = CALL_STATE.byteOffset(
            MemoryLayout.PathElement.groupElement("GetLastError")
    );
    private static final MemoryLayout LARGE_INTEGER = MemoryLayout.unionLayout(
            JAVA_LONG, MemoryLayout.structLayout(JAVA_INT, JAVA_INT)
    );

    private static final MethodHandle OPEN = bind("CreateFileW",
            FunctionDescriptor.of(ADDRESS, ADDRESS, JAVA_INT, JAVA_INT,
                    ADDRESS, JAVA_INT, JAVA_INT, ADDRESS));
    private static final MethodHandle ID = bind("GetFileInformationByHandleEx",
            FunctionDescriptor.of(JAVA_INT, ADDRESS, JAVA_INT, ADDRESS, JAVA_INT));
    private static final MethodHandle SIZE = bind("GetFileSizeEx",
            FunctionDescriptor.of(JAVA_INT, ADDRESS, ADDRESS));
    private static final MethodHandle SEEK = bind("SetFilePointerEx",
            FunctionDescriptor.of(JAVA_INT, ADDRESS, LARGE_INTEGER, ADDRESS, JAVA_INT));
    private static final MethodHandle READ = bind("ReadFile",
            FunctionDescriptor.of(JAVA_INT, ADDRESS, ADDRESS, JAVA_INT, ADDRESS, ADDRESS));
    private static final MethodHandle CLOSE = bind("CloseHandle",
            FunctionDescriptor.of(JAVA_INT, ADDRESS));

    private final Path path;
    private final MemorySegment handle;
    private final String id;
    private boolean closed;

    private WindowsLogFile(Path path, MemorySegment handle, String id) {
        this.path = path;
        this.handle = handle;
        this.id = id;
    }

    static WindowsLogFile open(Path path) throws IOException {
        Path absolute = path.toAbsolutePath().normalize();
        try (Arena arena = Arena.ofConfined()) {
            var state = arena.allocate(CALL_STATE);
            var name = arena.allocateFrom(absolute.toString(), UTF_16LE);
            var handle = (MemorySegment) call(OPEN, state, name, GENERIC_READ,
                    FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
                    NULL, OPEN_EXISTING, FILE_ATTRIBUTE_NORMAL, NULL);
            if (handle.address() == -1L) {
                throw failure(absolute, "CreateFileW", state);
            }

            try {
                var info = arena.allocate(FILE_ID_INFO_BYTES, Long.BYTES);
                if ((int) call(ID, state, handle, FILE_ID_INFO, info, FILE_ID_INFO_BYTES) == 0) {
                    throw failure(absolute, "GetFileInformationByHandleEx", state);
                }

                String volume = Long.toUnsignedString(info.get(JAVA_LONG, 0), 16);
                String fileId = HexFormat.of().formatHex(
                        info.asSlice(8, 16).toArray(JAVA_BYTE)
                );
                return new WindowsLogFile(absolute, handle, "ntfs:" + volume + ":" + fileId);
            } catch (IOException | RuntimeException | Error error) {
                try {
                    closeHandle(absolute, handle, state);
                } catch (IOException closeError) {
                    error.addSuppressed(closeError);
                }
                throw error;
            }
        }
    }

    @Override
    public String id() {
        return id;
    }

    @Override
    public long size() throws IOException {
        try (Arena arena = Arena.ofConfined()) {
            var state = arena.allocate(CALL_STATE);
            var length = arena.allocate(JAVA_LONG);
            if ((int) call(SIZE, state, handle, length) == 0) {
                throw failure(path, "GetFileSizeEx", state);
            }
            return length.get(JAVA_LONG, 0);
        }
    }

    @Override
    public int readAt(long offset, ByteBuffer destination) throws IOException {
        if (offset < 0) {
            throw new IllegalArgumentException("입력 위치가 음수입니다.");
        }
        int requested = Math.min(destination.remaining(), 8192);
        if (requested == 0) {
            return 0;
        }

        // 입력 객체는 수집 스레드 한 곳에서만 사용한다.
        try (Arena arena = Arena.ofConfined()) {
            var state = arena.allocate(CALL_STATE);
            var position = arena.allocate(LARGE_INTEGER);
            position.set(JAVA_LONG, 0, offset);
            if ((int) call(SEEK, state, handle, position, NULL, FILE_BEGIN) == 0) {
                throw failure(path, "SetFilePointerEx", state);
            }

            var bytes = arena.allocate(requested);
            var count = arena.allocate(JAVA_INT);
            if ((int) call(READ, state, handle, bytes, requested, count, NULL) == 0) {
                if (state.get(JAVA_INT, LAST_ERROR) == ERROR_HANDLE_EOF) {
                    return -1;
                }
                throw failure(path, "ReadFile", state);
            }

            int read = count.get(JAVA_INT, 0);
            if (read == 0) {
                return -1;
            }
            destination.put(bytes.asSlice(0, read).asByteBuffer());
            return read;
        }
    }

    @Override
    public void close() throws IOException {
        if (!closed) {
            try (Arena arena = Arena.ofConfined()) {
                closeHandle(path, handle, arena.allocate(CALL_STATE));
                closed = true;
            }
        }
    }

    private static void closeHandle(Path path, MemorySegment handle, MemorySegment state)
            throws IOException {
        if ((int) call(CLOSE, state, handle) == 0) {
            throw failure(path, "CloseHandle", state);
        }
    }

    private static MethodHandle bind(String name, FunctionDescriptor descriptor) {
        return LINKER.downcallHandle(KERNEL32.findOrThrow(name), descriptor,
                Linker.Option.captureCallState("GetLastError"));
    }

    private static Object call(MethodHandle method, Object... arguments) throws IOException {
        try {
            return method.invokeWithArguments(arguments);
        } catch (RuntimeException | Error error) {
            throw error;
        } catch (Throwable error) {
            throw new IOException("Win32 함수 호출에 실패했습니다.", error);
        }
    }

    private static IOException failure(Path path, String operation, MemorySegment state) {
        int code = state.get(JAVA_INT, LAST_ERROR);
        String reason = operation + " 실패, 윈도우 오류 코드 " + code;
        return switch (code) {
            case 2, 3 -> new NoSuchFileException(path.toString(), null, reason);
            case 5 -> new AccessDeniedException(path.toString(), null, reason);
            default -> new IOException(reason + " " + path);
        };
    }
}