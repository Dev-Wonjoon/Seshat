package com.seshat.ingestion.dto;

import jakarta.validation.Validation;
import jakarta.validation.Validator;
import jakarta.validation.ValidatorFactory;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.json.JsonMapper;

import java.util.List;
import java.util.UUID;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class LogBatchRequestTest {

    private static ValidatorFactory factory;
    private static Validator validator;

    private final JsonMapper mapper = JsonMapper.builder().build();

    @BeforeAll
    static void setUpValidator() {
        factory = Validation.buildDefaultValidatorFactory();
        validator = factory.getValidator();
    }

    @AfterAll
    static void closeValidator() {
        factory.close();
    }

    @Test
    void JSON_필드명이_DTO에_올바르게_연결된다() {
        String json = """
            {
                "run_id": "run-001",
                "source_id": "source-01",
                "mode": "AGENT",
                "events": [{
                    "event_id": "550e8400-e29b-41d4-a716-446655440000",
                    "raw": "invalid json",
                    "parse_status": "FAILED",
                    "parse_error": {
                        "code": "INVALID_JSON",
                        "message": "Invalid JSON syntax."
                    }
                }]
            }
        """;

        var request = mapper.readValue(json, LogBatchRequest.class);

        assertEquals("run-001", request.runId());
        assertEquals("source-01", request.sourceId());
        assertEquals("INVALID_JSON",
                    request.events().getFirst().parseError().code()
        );
        assertValid(request);
    }

    @Test
    void 중앙_모드에서는_원문의_JSON_문법을_검사하지_않는다() {
        for(String raw : List.of("", " ", "{\"message\":")) {
            var event = new LogEventRequest(
                    new UUID(0, 1), raw, null, null, null
            );

            assertValid(batch(ParsingMode.CENTRAL, List.of(event)));
        }
    }

    private LogBatchRequest batch(
            ParsingMode mode,
            List<LogEventRequest> events
    ) {
        return new LogBatchRequest("run-001", "source-01", mode, events);
    }

    private List<LogEventRequest> centralEvents(int count) {
        return IntStream.range(0, count)
                .mapToObj(index -> new LogEventRequest(
                        new UUID(0, index + 1),
                        "{}",
                        null,
                        null,
                        null
                )).toList();
    }

    private void assertValid(LogBatchRequest request) {
        var violations = validator.validate(request);

        assertTrue(violations.isEmpty(), () -> "Unexpected violations: " + violations);
    }

    private void assertRejectedAt(
            LogBatchRequest request,
            String property
    ) {
        var violations = validator.validate(request);

        assertTrue(
                violations.stream().anyMatch(
                        violation -> violation.getPropertyPath()
                                .toString().equals(property)
                ),
                () -> "Excepted violation at " + property + ", actual: " + violations
        );
    }
}
