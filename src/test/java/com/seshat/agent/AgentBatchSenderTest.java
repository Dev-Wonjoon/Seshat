package com.seshat.agent;

import com.seshat.ingestion.dto.LogBatchResponse;
import com.seshat.ingestion.dto.LogEventRequest;
import com.seshat.ingestion.dto.ParsingMode;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

@Timeout(15)
class AgentBatchSenderTest {

    private static final int MAX_REQUEST_BYTES = 1_048_576;

    private final JsonMapper mapper = JsonMapper.builder().build();

    private final BlockingQueue<Received> received =
            new LinkedBlockingQueue<>();

    private final BlockingQueue<Reply> replies =
            new LinkedBlockingQueue<>();

    private AgentBuffer buffer;
    private AgentBatchSender sender;
    private HttpServer server;

    @BeforeEach
    void setUp() throws IOException {
        buffer = new AgentBuffer(100);

        server = HttpServer.create(
                new InetSocketAddress("127.0.0.1", 0),
                0
        );

        server.createContext(
                "/api/logs/batches",
                this::handleRequest
        );

        server.start();

        URI endpoint = URI.create(
                "http://127.0.0.1:"
                        + server.getAddress().getPort()
                        + "/api/logs/batches"
        );

        sender = new AgentBatchSender(
                buffer,
                endpoint,
                "run-sender-test",
                "source-test",
                ParsingMode.CENTRAL,
                MAX_REQUEST_BYTES
        );
    }

    @AfterEach
    void tearDown() {
        // 실패한 테스트의 열린 연결도 먼저 해제한다.
        if (server != null) {
            server.stop(0);
        }

        if (sender != null) {
            sender.close();
        }
    }

    @Test
    void 이벤트_51건을_50건과_1건으로_전송한다()
            throws Exception {

        var expectedIds = new ArrayList<String>();

        for (int index = 0; index < 51; index++) {
            var event = enqueue(
                    "{\"sequence\":" + index + "}"
            );

            expectedIds.add(event.eventId().toString());
        }

        assertEquals(50, sender.runOnce());

        var first = takeRequest();

        assertEquals(50, eventCount(first));
        assertEquals(51, first.unacknowledgedBeforeReply());
        assertEquals(1, buffer.unacknowledgedCount());

        assertEquals(1, sender.runOnce());

        var second = takeRequest();

        assertEquals(1, eventCount(second));
        assertEquals(0, buffer.unacknowledgedCount());

        var actualIds = new ArrayList<String>();
        actualIds.addAll(eventIds(first));
        actualIds.addAll(eventIds(second));

        assertEquals(expectedIds, actualIds);
        assertTrue(received.isEmpty());
    }

    @Test
    void 저부하에서도_추가_입력없이_잔여_이벤트를_전송한다()
            throws Exception {

        long startedAt = System.nanoTime();

        var event = enqueue("{\"message\":\"single\"}");

        assertEquals(1, sender.runOnce());

        var request = takeRequest();

        long elapsed = request.receivedAtNanos() - startedAt;

        assertEquals(1, eventCount(request));
        assertEquals(
                List.of(event.eventId().toString()),
                eventIds(request)
        );

        // 한 건만 있을 때 배치 형성 대기를 거쳤는지 확인한다.
        assertTrue(
                elapsed >= TimeUnit.MILLISECONDS.toNanos(200),
                "The single-event batch was sent too early."
        );

        // 정확한 200ms 상한 대신 HTTP·실행 환경의 여유를 둔다.
        assertTrue(
                elapsed < TimeUnit.SECONDS.toNanos(5),
                "The remaining event was not sent promptly."
        );

        assertEquals(0, buffer.unacknowledgedCount());
    }

    @Test
    void UTF8_직렬화_크기가_제한을_넘으면_분할한다()
            throws Exception {

        // 한 이벤트는 약 600KB.
        // 두 이벤트의 문자 수는 1MiB 미만이지만 UTF-8 바이트는 초과한다.
        String raw = "{\"message\":\""
                + "가".repeat(200_000)
                + "\"}";

        var firstEvent = enqueue(raw);
        var secondEvent = enqueue(raw);

        assertEquals(1, sender.runOnce());

        var first = takeRequest();

        assertTrue(first.bytes().length <= MAX_REQUEST_BYTES);
        assertEquals(
                List.of(firstEvent.eventId().toString()),
                eventIds(first)
        );

        assertEquals(1, buffer.unacknowledgedCount());

        assertEquals(1, sender.runOnce());

        var second = takeRequest();

        assertTrue(second.bytes().length <= MAX_REQUEST_BYTES);
        assertEquals(
                List.of(secondEvent.eventId().toString()),
                eventIds(second)
        );

        assertEquals(
                raw,
                first.body()
                        .path("events")
                        .get(0)
                        .path("raw")
                        .stringValue()
        );

        assertEquals(0, buffer.unacknowledgedCount());
    }

    @Test
    void 일시_오류에는_같은_본문을_재전송하고_중복_확인도_수용한다()
            throws Exception {

        enqueue("{\"message\":\"retry\"}");

        replies.add(new Reply(503, "{}"));

        replies.add(new Reply(
                202,
                """
                        {
                          "accepted_count": 1,
                          "inserted_count": 0,
                          "duplicate_count": 1
                        }
                        """
        ));

        assertEquals(1, sender.runOnce());

        var first = takeRequest();
        var second = takeRequest();

        // ID, 원문, 모드를 포함한 직렬화 본문 전체가 같아야 한다.
        assertArrayEquals(first.bytes(), second.bytes());

        // 성공 응답을 받기 전까지 용량을 유지한다.
        assertEquals(1, first.unacknowledgedBeforeReply());
        assertEquals(1, second.unacknowledgedBeforeReply());

        assertEquals(0, buffer.unacknowledgedCount());
        assertTrue(received.isEmpty());
    }

    @Test
    void HTTP_202라도_응답_건수가_맞지_않으면_재전송한다()
            throws Exception {

        enqueue("{\"message\":\"invalid-ack\"}");

        // accepted_count는 맞지만 inserted + duplicate가 틀린 응답.
        replies.add(new Reply(
                202,
                """
                        {
                          "accepted_count": 1,
                          "inserted_count": 0,
                          "duplicate_count": 0
                        }
                        """
        ));

        // 다음 요청에는 테스트 서버가 기본 정상 응답을 반환한다.
        assertEquals(1, sender.runOnce());

        var first = takeRequest();
        var second = takeRequest();

        assertArrayEquals(first.bytes(), second.bytes());
        assertEquals(1, second.unacknowledgedBeforeReply());
        assertEquals(0, buffer.unacknowledgedCount());
        assertTrue(received.isEmpty());
    }

    @Test
    void HTTP_409는_예외로_전달하고_용량을_반환하지_않는다()
            throws Exception {

        enqueue("{\"message\":\"conflict\"}");
        replies.add(new Reply(409, "{}"));

        var exception = assertThrows(
                IllegalStateException.class,
                () -> sender.runOnce()
        );

        assertTrue(exception.getMessage().contains("409"));
        assertEquals(1, buffer.unacknowledgedCount());
        assertEquals(1, eventCount(takeRequest()));
        assertTrue(received.isEmpty());
    }

    @Test
    void 단일_이벤트가_너무_크면_HTTP_전송없이_거절한다()
            throws Exception {

        String raw = "{\"message\":\""
                + "x".repeat(MAX_REQUEST_BYTES)
                + "\"}";

        var event = enqueue(raw);

        var exception = assertThrows(
                IllegalStateException.class,
                () -> sender.runOnce()
        );

        assertTrue(
                exception.getMessage()
                        .contains(event.eventId().toString())
        );

        assertTrue(received.isEmpty());
        assertEquals(1, buffer.unacknowledgedCount());
    }

    private LogEventRequest enqueue(String raw)
            throws InterruptedException {

        var event = new LogEventRequest(
                UUID.randomUUID(),
                raw,
                null,
                null,
                null
        );

        buffer.submit(() -> event);
        return event;
    }

    private void handleRequest(HttpExchange exchange)
            throws IOException {

        try {
            byte[] bytes =
                    exchange.getRequestBody().readAllBytes();

            JsonNode body = mapper.readTree(bytes);

            received.add(new Received(
                    bytes,
                    body,
                    buffer.unacknowledgedCount(),
                    System.nanoTime()
            ));

            Reply reply = replies.poll();

            int status;
            byte[] response;

            if (reply == null) {
                int count = body.path("events").size();

                status = 202;
                response = mapper.writeValueAsBytes(
                        new LogBatchResponse(count, count, 0)
                );
            } else {
                status = reply.status();
                response = reply.body().getBytes(
                        StandardCharsets.UTF_8
                );
            }

            exchange.getResponseHeaders().set(
                    "Content-Type",
                    "application/json"
            );

            exchange.sendResponseHeaders(
                    status,
                    response.length
            );

            exchange.getResponseBody().write(response);
        } finally {
            exchange.close();
        }
    }

    private Received takeRequest() throws InterruptedException {
        var request = received.poll(3, TimeUnit.SECONDS);
        assertNotNull(request, "Expected an HTTP request.");
        return request;
    }

    private int eventCount(Received request) {
        return request.body().path("events").size();
    }

    private List<String> eventIds(Received request) {
        var events = request.body().path("events");
        var ids = new ArrayList<String>();

        for (int index = 0; index < events.size(); index++) {
            ids.add(
                    events.get(index)
                            .path("event_id")
                            .stringValue()
            );
        }

        return ids;
    }

    private record Received(
            byte[] bytes,
            JsonNode body,
            int unacknowledgedBeforeReply,
            long receivedAtNanos
    ) {
    }

    private record Reply(int status, String body) {
    }
}
