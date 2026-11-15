package com.seshat.agent;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;

import static java.nio.file.LinkOption.NOFOLLOW_LINKS;
import static java.nio.file.StandardOpenOption.READ;

record NioLogFile(String id, FileChannel channel) implements AgentInputFile {
    static NioLogFile open(Path path) throws IOException {
        for (int attempt = 0; attempt < 3; attempt++) {
            Object before = fileKey(path);
            FileChannel channel = FileChannel.open(path, READ, NOFOLLOW_LINKS);
            boolean accepted = false;
            try {
                if (before.equals(fileKey(path))) {
                    accepted = true;
                    return new NioLogFile("linux:" + before, channel);
                }
            } catch (NoSuchFileException ignored) {
                // 채널을 여는 동안 경로가 이동했다.
            } finally {
                if (!accepted) {
                    channel.close();
                }
            }
        }
        throw new IOException("입력 경로가 연속으로 교체됐습니다. " + path);
    }

    private static Object fileKey(Path path) throws IOException {
        var attributes = Files.readAttributes(
                path, BasicFileAttributes.class, NOFOLLOW_LINKS
        );
        Object key = attributes.fileKey();
        if (!attributes.isRegularFile() || key == null) {
            throw new IOException("파일 키가 있는 일반 파일이 필요합니다. " + path);
        }
        return key;
    }

    @Override
    public long size() throws IOException {
        return channel.size();
    }

    @Override
    public int readAt(long offset, ByteBuffer destination) throws IOException {
        return channel.read(destination, offset);
    }

    @Override
    public void close() throws IOException {
        channel.close();
    }
}