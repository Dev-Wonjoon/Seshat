package com.seshat.processing;

import tools.jackson.core.JacksonException;
import tools.jackson.core.StreamReadConstraints;
import tools.jackson.core.StreamReadFeature;
import tools.jackson.core.exc.StreamConstraintsException;
import tools.jackson.core.json.JsonFactory;
import tools.jackson.databind.DeserializationFeature;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.node.ObjectNode;

import java.util.Objects;

public final class JavaLogProcessor {

    // 초기 제안값. 중앙과 에이전트에 동일하게 적용한다.
    private static final int MAX_NESTING_DEPTH = 50;
    private static final int MAX_NUMBER_LENGTH = 1_000;

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

        ObjectNode result = mapper.createObjectNode();
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