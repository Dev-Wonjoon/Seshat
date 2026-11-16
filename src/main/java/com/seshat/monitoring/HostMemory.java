package com.seshat.monitoring;

import com.sun.management.OperatingSystemMXBean;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.regex.Pattern;

public record HostMemory(long usedBytes, long totalBytes) {
    private static final Path MEMINFO = Path.of("/proc/meminfo");
    private static final Pattern LINUX_MEMORY_FIELD =
            Pattern.compile("^(MemTotal|MemAvailable):\\s+(\\d+)\\s+kB\\s*$");

    public static HostMemory read() {
        try {
            if ("Linux".equals(System.getProperty("os.name"))) {
                return fromMeminfo(Files.readAllLines(MEMINFO));
            }

            if (ManagementFactory.getOperatingSystemMXBean() instanceof OperatingSystemMXBean os) {
                return fromAvailable(os.getTotalMemorySize(), os.getFreeMemorySize());
            }
        } catch (IOException | RuntimeException ignored) {
            // 메모리 표본을 읽지 못해도 다른 시스템 지표는 계속 수집한다.
        }
        return null;
    }

    static HostMemory fromMeminfo(List<String> lines) {
        long totalKb = -1;
        long availableKb = -1;
        for (String line : lines) {
            var field = LINUX_MEMORY_FIELD.matcher(line);
            if (!field.matches()) {
                continue;
            }

            long value = Long.parseLong(field.group(2));
            if (field.group(1).equals("MemTotal")) {
                totalKb = value;
            } else {
                availableKb = value;
            }
        }

        return fromAvailable(Math.multiplyExact(totalKb, 1024L),
                Math.multiplyExact(availableKb, 1024L));
    }

    private static HostMemory fromAvailable(long total, long available) {
        return total > 0 && available >= 0 && available <= total
                ? new HostMemory(total - available, total)
                : null;
    }
}
