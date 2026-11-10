package com.seshat.ingestion.filter;

import org.junit.jupiter.api.Test;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;
import tools.jackson.databind.json.JsonMapper;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.*;

class BatchRequestSizeFilterTest {

    private static final int MAX_BYTES = 512;

    private final JsonMapper mapper = JsonMapper.builder().build();

    private final BatchRequestSizeFilter filter =
            new BatchRequestSizeFilter(MAX_BYTES, mapper);

    @Test
    void 상한과_정확히_같은_크기는_본문을_유지하여_전달한다()
            throws Exception {

        assertForwarded(asciiBody(MAX_BYTES), false);
    }

    @Test
    void 상한보다_1바이트_크면_413을_반환한다()
            throws Exception {

        assertRejected(asciiBody(MAX_BYTES + 1), false);
    }

    @Test
    void 길이를_몰라도_상한_이내이면_전달한다()
            throws Exception {

        assertForwarded(asciiBody(MAX_BYTES), true);
    }

    @Test
    void 길이를_몰라도_상한을_초과하면_413을_반환한다()
            throws Exception {

        assertRejected(asciiBody(MAX_BYTES + 1), true);
    }

    @Test
    void 문자_수가_작아도_UTF8_바이트가_상한을_넘으면_거절한다()
            throws Exception {

        String json = "{\"message\":\"" + "한".repeat(200) + "\"}";
        byte[] body = json.getBytes(StandardCharsets.UTF_8);

        assertTrue(json.length() < MAX_BYTES);
        assertTrue(body.length > MAX_BYTES);

        assertRejected(body, true);
    }

    @Test
    void 상한_이내의_한글_본문을_그대로_전달한다()
            throws Exception {

        String json = "{\"message\":\"" + "한".repeat(100) + "\"}";
        byte[] body = json.getBytes(StandardCharsets.UTF_8);

        assertTrue(body.length < MAX_BYTES);

        assertForwarded(body, false);
    }

    private void assertForwarded(
            byte[] body,
            boolean unknownLength
    ) throws Exception {
        var response = new MockHttpServletResponse();
        var reachedNext = new AtomicBoolean();

        filter.doFilter(
                request(body, unknownLength),
                response,
                (forwarded, servletResponse) -> {
                    reachedNext.set(true);

                    assertEquals(
                            (long) body.length,
                            forwarded.getContentLengthLong()
                    );
                    assertArrayEquals(
                            body,
                            forwarded.getInputStream().readAllBytes()
                    );
                }
        );

        assertTrue(reachedNext.get());
    }

    private void assertRejected(
            byte[] body,
            boolean unknownLength
    ) throws Exception {
        var response = new MockHttpServletResponse();
        var reachedNext = new AtomicBoolean();

        filter.doFilter(
                request(body, unknownLength),
                response,
                (forwarded, servletResponse) -> reachedNext.set(true)
        );

        assertFalse(reachedNext.get());
        assertEquals(413, response.getStatus());

        var error = mapper.readTree(response.getContentAsByteArray());

        assertEquals(
                "REQUEST_TOO_LARGE",
                error.path("code").stringValue()
        );
    }

    private MockHttpServletRequest request(
            byte[] body,
            boolean unknownLength
    ) {
        var request = new MockHttpServletRequest(
                "POST",
                "/api/logs/batches"
        ) {
            @Override
            public int getContentLength() {
                return unknownLength ? -1 : super.getContentLength();
            }

            @Override
            public long getContentLengthLong() {
                return unknownLength ? -1 : super.getContentLengthLong();
            }
        };

        request.setContentType("application/json");
        request.setCharacterEncoding(StandardCharsets.UTF_8.name());
        request.setContent(body);

        return request;
    }

    private byte[] asciiBody(int size) {
        // 공백과 객체로 구성한 유효한 JSON. ASCII이므로 문자당 1바이트다.
        return (" ".repeat(size - 2) + "{}")
                .getBytes(StandardCharsets.UTF_8);
    }
}