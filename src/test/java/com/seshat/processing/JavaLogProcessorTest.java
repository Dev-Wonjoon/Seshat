package com.seshat.processing;

import com.seshat.ingestion.dto.LogFormat;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class JavaLogProcessorTest {
    private final JavaLogProcessor processor = new JavaLogProcessor();

    @Test
    void AUTO와_명시적_선택이_Minecraft_헤더와_메시지를_같게_파싱한다() {
        String raw = "[23:52:17] [worker/io/INFO]: 청크를 저장했습니다: [2] ";
        var result = processor.process(raw, LogFormat.MINECRAFT);
        assertEquals(result, processor.process(raw, LogFormat.AUTO));
        assertEquals(raw, result.path("raw").stringValue());
        assertEquals("23:52:17", result.at("/parsed/time").stringValue());
        assertEquals("worker/io", result.at("/parsed/thread").stringValue());
        assertEquals("INFO", result.at("/parsed/level").stringValue());
        assertEquals("청크를 저장했습니다: [2] ", result.at("/parsed/message").stringValue());
    }

    @Test
    void Spring_Boot_기본_헤더의_선택적_앱_이름과_스레드를_구분한다() {
        String timestamp = "2026-11-16T23:52:17.123+09:00";
        String raw = timestamp + "  INFO 1234 --- [seshat] [   main] c.s.App     : Started: ok";
        var result = processor.process(raw, LogFormat.SPRING_BOOT);
        assertEquals(result, processor.process(raw, LogFormat.AUTO));
        assertEquals(timestamp, result.at("/parsed/timestamp").stringValue());
        assertEquals("INFO", result.at("/parsed/level").stringValue());
        assertEquals(1234, result.at("/parsed/pid").intValue());
        assertEquals("seshat", result.at("/parsed/application").stringValue());
        assertEquals("main", result.at("/parsed/thread").stringValue());
        assertEquals("c.s.App", result.at("/parsed/logger").stringValue());
        assertEquals("Started: ok", result.at("/parsed/message").stringValue());

        var unnamed = processor.process(timestamp + " WARN 1234 --- [ main] c.s.App : Warning", LogFormat.AUTO);
        assertEquals("main", unnamed.at("/parsed/thread").stringValue());
        assertTrue(unnamed.at("/parsed/application").isMissingNode());
    }

    @Test
    void NGINX_combined_접근_로그와_표준_오류_로그를_자동으로_파싱한다() {
        String access = "192.0.2.10 - - [16/Nov/2026:18:30:00 +0900]"
                + " \"GET /api/logs HTTP/1.1\" 503 124 \"-\" \"Agent \\x22demo\\x22\"";
        var accessResult = processor.process(access, LogFormat.NGINX);
        assertEquals(accessResult, processor.process(access, LogFormat.AUTO));
        assertEquals("access", accessResult.at("/parsed/kind").stringValue());
        assertEquals("192.0.2.10", accessResult.at("/parsed/client_ip").stringValue());
        assertEquals("16/Nov/2026:18:30:00 +0900", accessResult.at("/parsed/timestamp").stringValue());
        assertEquals("GET /api/logs HTTP/1.1", accessResult.at("/parsed/request").stringValue());
        assertEquals(503, accessResult.at("/parsed/status").intValue());
        assertEquals(124, accessResult.at("/parsed/body_bytes_sent").intValue());
        assertEquals("Agent \\x22demo\\x22", accessResult.at("/parsed/user_agent").stringValue());
        assertTrue(accessResult.at("/parsed/extra").isMissingNode());

        String error = "2026/11/16 18:30:00 [error] 42#92: *9 connect() failed, client: 192.0.2.10";
        var errorResult = processor.process(error, LogFormat.NGINX);
        assertEquals(errorResult, processor.process(error, LogFormat.AUTO));
        assertEquals("error", errorResult.at("/parsed/kind").stringValue());
        assertEquals("ERROR", errorResult.at("/parsed/level").stringValue());
        assertEquals(42, errorResult.at("/parsed/pid").intValue());
        assertEquals(92, errorResult.at("/parsed/tid").intValue());
        assertEquals(9, errorResult.at("/parsed/connection_id").intValue());
        assertEquals("connect() failed, client: 192.0.2.10", errorResult.at("/parsed/message").stringValue());
    }

    @Test
    void NGINX_combined_뒤의_선택적_따옴표_필드를_보존한다() {
        String combined = "192.0.2.10 - - [16/Nov/2026:19:48:05 +0900]"
                + " \"GET /api/queue/traffic HTTP/1.1\" 200 522"
                + " \"https\\://example.test/\" \"Mozilla/5.0 (Windows NT 10.0; Win64; x64)\"";
        for (String extra : List.of("192.0.2.10", "192.0.2.10, 2001:db8::1", "")) {
            String raw = combined + " \"" + extra + "\"";
            var result = processor.process(raw, LogFormat.NGINX);
            assertEquals("SUCCESS", result.path("parse_status").stringValue());
            assertEquals(result, processor.process(raw, LogFormat.AUTO));
            assertEquals(raw, result.path("raw").stringValue());
            assertEquals("https\\://example.test/", result.at("/parsed/referer").stringValue());
            assertEquals(200, result.at("/parsed/status").intValue());
            assertEquals(extra, result.at("/parsed/extra").stringValue());
        }
        assertEquals("FORMAT_MISMATCH",
                processor.process(combined + " \"one\" \"two\"", LogFormat.NGINX)
                        .at("/parse_error/code").stringValue());
    }

    @Test
    void AUTO에서_JSON과_미지원_형식과_잘못된_형식을_구분한다() {
        String valid = "  {\"message\":\"hello\"}  ";
        assertEquals(processor.process(valid), processor.process(valid, LogFormat.AUTO));
        assertEquals("INVALID_JSON", processor.process("{", LogFormat.AUTO).at("/parse_error/code").stringValue());
        assertEquals("UNRECOGNIZED_FORMAT",
                processor.process("unknown format", LogFormat.AUTO).at("/parse_error/code").stringValue());
        assertEquals("FORMAT_MISMATCH",
                processor.process(valid, LogFormat.MINECRAFT).at("/parse_error/code").stringValue());
    }

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
