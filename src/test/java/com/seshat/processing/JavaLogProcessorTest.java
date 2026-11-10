package com.seshat.processing;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class JavaLogProcessorTest {
    private final JavaLogProcessor processor = new JavaLogProcessor();

    @Test
    void 업무_필드가_없어도_성공하고_원문을_보존한다() {
        String raw = "  {\"message\":\"안녕\"}  ";

        var result = processor.process(raw);

        assertEquals("SUCCESS", result.path("parse_status").stringValue());
        assertEquals(raw, result.path("raw").stringValue());
        assertEquals("안녕", result.at("/parsed/message").stringValue());
        assertFalse(result.has("parse_error"));
    }

    @Test
    void 업무_값을_변경하거나_기본값을_추가하지_않는다() {
        var result = processor.process("{\"level\":\"info\"}");

        assertEquals("SUCCESS", result.path("parse_status").stringValue());
        assertEquals("info", result.at("/parsed/level").stringValue());
        assertFalse(result.path("parsed").has("event_time"));
    }

    @Test
    void 실패_입력의_원문을_보존하고_부분_결과를_반환하지_않는다() {
        for (String raw : List.of(
                "",
                "   ",
                "{\"message\":",
                "{\"a\":1} garbage",
                "{\"a\":1} {\"b\":2}",
                "{\"a\":1,\"a\":2}"
        )) {
            var result = processor.process(raw);

            assertEquals("FAILED", result.path("parse_status").stringValue(), raw);
            assertEquals("INVALID_JSON",
                    result.at("/parse_error/code").stringValue(), raw);
            assertEquals(raw, result.path("raw").stringValue());
            assertFalse(result.has("parsed"), raw);
        }
    }

    @Test
    void 객체가_아닌_최상위_값은_거절한다() {
        for(String raw : List.of("[]", "\"hello\"", "123", "true", "null")) {
            var result = processor.process(raw);

            assertEquals("FAILED",
                    result.path("parse_status").stringValue(), raw);

            assertEquals("UNSUPPORTED_ROOT_TYPE",
                    result.at("/parse_error/code").stringValue(), raw);

            assertEquals(raw, result.path("raw").stringValue());
            assertFalse(result.has("parsed"));
        }
    }

    @Test
    void 객체_내부의_다양한_JSON_값을_보존한다() {
        String raw = """
            {"items":[1,"한글",true,null,{"value":"ok"}]}
        """;

        var result = processor.process(raw);

        assertEquals("SUCCESS",
                result.path("parse_status").stringValue());
        assertEquals(5, result.at("/parsed/items").size());
        assertEquals("한글", result.at("/parsed/items/1").stringValue());
        assertTrue(result.at("/parsed/items/3").isNull());
        assertEquals("ok", result.at("/parsed/items/4/value").stringValue());
    }

    @Test
    void 숫자의_정밀도를_보존한다() {
        String integer = "123456789012345678901234567890";
        String decimal = "0.12345678901234567890123456789";

        String raw = "{\"integer\":" + integer
                + ",\"decimal\":" + decimal + "}";

        var result = processor.process(raw);

        assertEquals("SUCCESS", result.path("parse_status").stringValue());

        assertEquals(
                new BigInteger(integer),
                result.at("/parsed/integer").decimalValue().toBigIntegerExact()
        );

        assertEquals(
                0,
                new BigDecimal(decimal).compareTo(
                        result.at("/parsed/decimal").decimalValue()
                )
        );
    }

    @Test
    void 중첩_깊이_50은_허용하고_51은_거절한다() {
        String allowed = "{\"a\":".repeat(49)
                + "{}" + "}".repeat(49);

        String exceeded = "{\"a\":".repeat(50)
                + "{}" + "}".repeat(50);

        var success = processor.process(allowed);
        var failure = processor.process(exceeded);

        assertEquals("SUCCESS", success.path("parse_status").stringValue());

        assertEquals("FAILED", failure.path("parse_status").stringValue());
        assertEquals(
                "JSON_LIMIT_EXCEEDED",
                failure.at("/parse_error/code").stringValue()
        );
        assertEquals(exceeded, failure.path("raw").stringValue());
        assertFalse(failure.has("parsed"));
    }
}