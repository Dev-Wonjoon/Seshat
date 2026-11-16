package com.seshat.processing;

import com.seshat.ingestion.dto.LogFormat;
import tools.jackson.core.JacksonException;
import tools.jackson.core.StreamReadConstraints;
import tools.jackson.core.StreamReadFeature;
import tools.jackson.core.exc.StreamConstraintsException;
import tools.jackson.core.json.JsonFactory;
import tools.jackson.databind.DeserializationFeature;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.node.ObjectNode;

import java.math.BigInteger;
import java.util.Locale;
import java.util.Objects;
import java.util.regex.Pattern;

public final class JavaLogProcessor {

    // 초기 제안값. 중앙과 에이전트에 동일하게 적용한다.
    private static final int MAX_NESTING_DEPTH = 50;
    private static final int MAX_NUMBER_LENGTH = 1_000;
    private static final int DETECTION_PREFIX_LENGTH = 512;
    private static final String TIME = "[0-9]{2}:[0-9]{2}:[0-9]{2}";
    private static final String LEVEL = "TRACE|DEBUG|INFO|WARN|ERROR|FATAL";

    private static final Pattern MINECRAFT = Pattern.compile(
            "\\[(" + TIME + ")\\] \\[([^\\]\\r\\n]+)/(" + LEVEL + ")\\]: ?"
    );
    private static final Pattern SPRING_BOOT = Pattern.compile(
            "([0-9]{4}-[0-9]{2}-[0-9]{2}[T ]" + TIME
                    + "(?:[.,][0-9]{1,9})?(?:Z|[+-][0-9]{2}:?[0-9]{2})?)\\h+("
                    + LEVEL + ")\\h+(?:([0-9]{1,20})\\h+)?---\\h+"
                    + "(?:\\[([^\\]\\r\\n]*)\\]\\h+)?"
                    + "\\[([^\\]\\r\\n]*)\\]\\h+([^\\s:]++)\\h*:\\h?"
    );
    private static final Pattern NGINX_ERROR = Pattern.compile(
            "([0-9]{4}/[0-9]{2}/[0-9]{2} " + TIME
                    + ") \\[(debug|info|notice|warn|error|crit|alert|emerg)\\] "
                    + "([0-9]{1,20})#([0-9]{1,20}): (?:\\*([0-9]{1,20}) )?"
    );
    private static final String NGINX_ACCESS_START =
            "(\\S{1,128}) - ([^\\[\\r\\n]+?) \\[([0-9]{2}/[A-Za-z]{3}/[0-9]{4}:" + TIME;
    private static final Pattern NGINX_ACCESS_HEADER = Pattern.compile(NGINX_ACCESS_START + ")");
    private static final String NGINX_QUOTED =
            "\"([^\"\\\\\\r\\n]*+(?:\\\\[^\\r\\n][^\"\\\\\\r\\n]*+)*+)\"";
    private static final Pattern NGINX_ACCESS = Pattern.compile(
            NGINX_ACCESS_START + " [+-][0-9]{4})\\] "
                    + NGINX_QUOTED + " ([0-9]{3}) ([0-9]{1,20}) "
                    + NGINX_QUOTED + " " + NGINX_QUOTED
                    + "(?: " + NGINX_QUOTED + ")?"
    );

    private final JsonMapper mapper = createMapper();

    private static JsonMapper createMapper() {
        var constraints = StreamReadConstraints.builder()
                .maxNestingDepth(MAX_NESTING_DEPTH)
                .maxNumberLength(MAX_NUMBER_LENGTH)
                .build();

        var factory = JsonFactory.builder()
                .streamReadConstraints(constraints)
                .enable(StreamReadFeature.STRICT_DUPLICATE_DETECTION)
                .build();

        return JsonMapper.builder(factory)
                .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS)
                .enable(DeserializationFeature.USE_BIG_INTEGER_FOR_INTS)
                .enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS)
                .build();
    }

    /**
     * raw는 줄 구분자를 제외한 원문 한 줄이다.
     * 호출자는 같은 인스턴스를 재사용한다.
     */
    public ObjectNode process(String raw) {
        Objects.requireNonNull(raw, "raw must not be null");

        JsonNode parsed;

        try {
            parsed = mapper.readTree(raw);
        } catch (StreamConstraintsException exception) {
            return failure(
                    raw,
                    "JSON_LIMIT_EXCEEDED",
                    "JSON input exceeds configured parser limits."
            );
        } catch (JacksonException exception) {
            return failure(
                    raw,
                    "INVALID_JSON",
                    "Invalid JSON syntax."
            );
        }

        if (parsed == null || parsed.isMissingNode()) {
            return failure(
                    raw,
                    "INVALID_JSON",
                    "JSON input is empty."
            );
        }

        if (!parsed.isObject()) {
            return failure(
                    raw,
                    "UNSUPPORTED_ROOT_TYPE",
                    "Expected a JSON object."
            );
        }

        return success(raw, parsed);
    }

    public ObjectNode process(String raw, LogFormat format) {
        Objects.requireNonNull(raw, "raw must not be null");
        Objects.requireNonNull(format, "format must not be null");
        if (format == LogFormat.JSON) {
            return process(raw);
        }

        int start = 0;
        while (start < raw.length() && Character.isWhitespace(raw.charAt(start))) {
            start++;
        }
        LogFormat selected = format == LogFormat.AUTO ? detect(raw, start) : format;
        if (selected == LogFormat.AUTO) {
            return failure(raw, "UNRECOGNIZED_FORMAT", "No supported log format matched the line.");
        }
        if (selected == LogFormat.JSON) {
            return process(raw);
        }

        var parsed = switch (selected) {
            case MINECRAFT -> parseMinecraft(raw, start);
            case SPRING_BOOT -> parseSpringBoot(raw, start);
            case NGINX -> parseNginx(raw, start);
            default -> throw new IllegalStateException("Unexpected log format: " + selected);
        };
        return parsed == null
                ? failure(raw, "FORMAT_MISMATCH", "The line does not match " + selected + ".")
                : success(raw, parsed);
    }

    private static LogFormat detect(String raw, int start) {
        if (start == raw.length()) {
            return LogFormat.AUTO;
        }
        char initial = raw.charAt(start);
        if (initial == '{') {
            return LogFormat.JSON;
        }
        int end = start + Math.min(raw.length() - start, DETECTION_PREFIX_LENGTH);
        if (initial == '[' && MINECRAFT.matcher(raw).region(start, end).lookingAt()) {
            return LogFormat.MINECRAFT;
        }
        if (SPRING_BOOT.matcher(raw).region(start, end).lookingAt()) {
            return LogFormat.SPRING_BOOT;
        }
        if (NGINX_ERROR.matcher(raw).region(start, end).lookingAt()
                || NGINX_ACCESS_HEADER.matcher(raw).region(start, end).lookingAt()) {
            return LogFormat.NGINX;
        }
        return LogFormat.AUTO;
    }

    private ObjectNode parseMinecraft(String raw, int start) {
        var header = MINECRAFT.matcher(raw).region(start, raw.length());
        if (!header.lookingAt()) {
            return null;
        }
        return mapper.createObjectNode()
                .put("time", header.group(1))
                .put("thread", header.group(2))
                .put("level", header.group(3))
                .put("message", raw.substring(header.end()));
    }

    private ObjectNode parseSpringBoot(String raw, int start) {
        var header = SPRING_BOOT.matcher(raw).region(start, raw.length());
        if (!header.lookingAt()) {
            return null;
        }
        var parsed = mapper.createObjectNode()
                .put("timestamp", header.group(1))
                .put("level", header.group(2))
                .put("thread", header.group(5).strip())
                .put("logger", header.group(6))
                .put("message", raw.substring(header.end()));
        if (header.group(3) != null) {
            parsed.put("pid", new BigInteger(header.group(3)));
        }
        if (header.group(4) != null) {
            parsed.put("application", header.group(4).strip());
        }
        return parsed;
    }

    private ObjectNode parseNginx(String raw, int start) {
        var error = NGINX_ERROR.matcher(raw).region(start, raw.length());
        if (error.lookingAt()) {
            var parsed = mapper.createObjectNode()
                    .put("kind", "error")
                    .put("timestamp", error.group(1))
                    .put("level", error.group(2).toUpperCase(Locale.ROOT))
                    .put("pid", new BigInteger(error.group(3)))
                    .put("tid", new BigInteger(error.group(4)))
                    .put("message", raw.substring(error.end()));
            if (error.group(5) != null) {
                parsed.put("connection_id", new BigInteger(error.group(5)));
            }
            return parsed;
        }
        var access = NGINX_ACCESS.matcher(raw).region(start, raw.length());
        if (!access.matches()) {
            return null;
        }
        var parsed = mapper.createObjectNode()
                .put("kind", "access")
                .put("client_ip", access.group(1))
                .put("remote_user", access.group(2))
                .put("timestamp", access.group(3))
                .put("request", access.group(4))
                .put("status", new BigInteger(access.group(5)))
                .put("body_bytes_sent", new BigInteger(access.group(6)))
                .put("referer", access.group(7))
                .put("user_agent", access.group(8));
        String extra = access.group(9);
        if (extra != null) {
            parsed.put("extra", extra);
        }
        return parsed;
    }

    private ObjectNode success(String raw, JsonNode parsed) {
        var result = mapper.createObjectNode();
        result.put("raw", raw);
        result.put("parse_status", "SUCCESS");
        result.set("parsed", parsed);
        return result;
    }

    private ObjectNode failure(
            String raw,
            String code,
            String message
    ) {
        ObjectNode error = mapper.createObjectNode();
        error.put("code", code);
        error.put("message", message);

        ObjectNode result = mapper.createObjectNode();
        result.put("raw", raw);
        result.put("parse_status", "FAILED");
        result.set("parse_error", error);

        return result;
    }
}
