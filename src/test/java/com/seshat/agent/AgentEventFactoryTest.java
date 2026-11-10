package com.seshat.agent;

import com.seshat.ingestion.dto.ParseStatus;
import com.seshat.ingestion.dto.ParsingMode;
import com.seshat.processing.JavaLogProcessor;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class AgentEventFactoryTest {

    private final AgentEventFactory factory =
            new AgentEventFactory(new JavaLogProcessor());

    @Test
    void CENTRAL_모드에서는_원문을_파싱하지_않는다() {
        var processor = mock(JavaLogProcessor.class);
        var centralFactory = new AgentEventFactory(processor);

        // 잘못된 JSON도 중앙 모드에서는 원문 그대로 전송한다.
        String raw = "  {\"message\":";

        var event = centralFactory.create(
                raw,
                ParsingMode.CENTRAL
        );

        assertNotNull(event.eventId());
        assertEquals(raw, event.raw());
        assertNull(event.parseStatus());
        assertNull(event.parsed());
        assertNull(event.parseError());

        verifyNoInteractions(processor);
    }

    @Test
    void AGENT_성공은_원문과_파싱_결과를_함께_담는다() {
        String raw =
                "  {\"level\":\"info\",\"message\":\"안녕\",\"value\":123}  ";

        var event = factory.create(
                raw,
                ParsingMode.AGENT
        );

        assertNotNull(event.eventId());
        assertEquals(raw, event.raw());
        assertEquals(ParseStatus.SUCCESS, event.parseStatus());
        assertNotNull(event.parsed());
        assertNull(event.parseError());

        assertEquals(
                "info",
                event.parsed().path("level").stringValue()
        );

        assertEquals(
                "안녕",
                event.parsed().path("message").stringValue()
        );

        assertEquals(
                123,
                event.parsed().path("value").intValue()
        );
    }

    @Test
    void AGENT_실패는_원문과_오류를_담고_다음_이벤트도_처리한다() {
        String raw = "{\"message\":";

        var failed = factory.create(
                raw,
                ParsingMode.AGENT
        );

        assertNotNull(failed.eventId());
        assertEquals(raw, failed.raw());
        assertEquals(ParseStatus.FAILED, failed.parseStatus());
        assertNull(failed.parsed());
        assertNotNull(failed.parseError());

        assertEquals(
                "INVALID_JSON",
                failed.parseError().code()
        );

        assertNotNull(failed.parseError().message());
        assertFalse(failed.parseError().message().isBlank());

        var next = factory.create(
                "{\"message\":\"next\"}",
                ParsingMode.AGENT
        );

        assertEquals(ParseStatus.SUCCESS, next.parseStatus());
        assertNotNull(next.parsed());
        assertNull(next.parseError());
    }

    @Test
    void 같은_원문도_새로_수집하면_서로_다른_이벤트_ID를_생성한다() {
        String raw =
                "{\"event_id\":\"business-id\",\"message\":\"same\"}";

        var first = factory.create(raw, ParsingMode.CENTRAL);
        var second = factory.create(raw, ParsingMode.CENTRAL);

        assertNotNull(first.eventId());
        assertNotNull(second.eventId());
        assertNotEquals(first.eventId(), second.eventId());

        assertEquals(raw, first.raw());
        assertEquals(raw, second.raw());
    }
}