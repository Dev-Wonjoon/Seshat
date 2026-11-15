package com.seshat.agent;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.Locale;

interface AgentInputFile extends AutoCloseable {
    static AgentInputFile open(Path path) throws IOException {
        String os = System.getProperty("os.name", "").toLowerCase(Locale.ROOT);
        if (os.startsWith("windows")) {
            if (!AgentInputFile.class.getModule().isNativeAccessEnabled()) {
                throw new IOException("윈도우 로그 수집에 JVM 네이티브 접근이 필요합니다.");
            }
            return WindowsLogFile.open(path);
        }
        if (os.equals("linux")) {
            return NioLogFile.open(path);
        }
        throw new IOException("지원하지 않는 운영체제입니다. " + os);
    }

    // 체크포인트에 저장할 파일시스템 식별자
    String id();

    long size() throws IOException;

    // 지정한 위치에서 읽고, EOF이면 -1을 반환한다.
    int readAt(long offset, ByteBuffer destination) throws IOException;

    @Override
    void close() throws IOException;
}