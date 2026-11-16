package com.seshat.agent;

import com.seshat.ingestion.dto.ParseStatus;
import com.seshat.ingestion.dto.LogFormat;
import com.seshat.ingestion.dto.ParsingMode;
import com.seshat.processing.JavaLogProcessor;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class AgentEventFactoryTest {

    private final AgentEventFactory factory =
            new AgentEventFactory(new JavaLogProcessor());

    @Test
    void 중앙에는_선택한_형식과_원문을_보내고_에이전트는_직접_파싱한다() {
        var configured = new AgentEventFactory(new JavaLogProcessor(), LogFormat.AUTO);
        String raw = "[23:52:17] [Server thread/INFO]: 저장 완료";
        var central = configured.create(raw, ParsingMode.CENTRAL);
        var agent = configured.create(central.eventId(), raw, ParsingMode.AGENT);
        assertEquals(LogFormat.AUTO, central.logFormat());
        assertEquals(raw, central.raw());
        assertNull(central.parsed());
        assertNull(central.parseStatus());
        assertEquals(LogFormat.AUTO, agent.logFormat());
        assertEquals(raw, agent.raw());
        assertEquals(ParseStatus.SUCCESS, agent.parseStatus());
        assertEquals("저장 완료", agent.parsed().path("message").stringValue());
        assertNull(factory.create("{}", ParsingMode.CENTRAL).logFormat());
    }

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

    @Test
    void 지정한_ID로_성공_이벤트의_모드를_바꿔도_ID와_원문을_유지한다() {
        UUID eventId = UUID.fromString(
                "11111111-1111-4111-8111-111111111111"
        );

        String raw = "  {\"message\":\"모드 전환\",\"value\":123}  ";

        var central = factory.create(
                eventId,
                raw,
                ParsingMode.CENTRAL
        );

        var agent = factory.create(
                central.eventId(),
                central.raw(),
                ParsingMode.AGENT
        );

        var centralAgain = factory.create(
                agent.eventId(),
                agent.raw(),
                ParsingMode.CENTRAL
        );

        assertAll(
                () -> assertEquals(eventId, central.eventId()),
                () -> assertEquals(eventId, agent.eventId()),
                () -> assertEquals(eventId, centralAgain.eventId()),
                () -> assertEquals(raw, central.raw()),
                () -> assertEquals(raw, agent.raw()),
                () -> assertEquals(raw, centralAgain.raw())
        );

        assertNull(central.parseStatus());
        assertNull(central.parsed());
        assertNull(central.parseError());

        assertEquals(ParseStatus.SUCCESS, agent.parseStatus());
        assertNotNull(agent.parsed());
        assertNull(agent.parseError());
        assertEquals(
                "모드 전환",
                agent.parsed().path("message").stringValue()
        );
        assertEquals(
                123,
                agent.parsed().path("value").intValue()
        );

        // CENTRAL용으로 다시 준비하면 AGENT의 결과를 싣지 않는다.
        assertNull(centralAgain.parseStatus());
        assertNull(centralAgain.parsed());
        assertNull(centralAgain.parseError());
    }

    @Test
    void 지정한_ID로_실패_이벤트의_모드를_바꿔도_ID와_원문을_유지한다() {
        UUID eventId = UUID.fromString(
                "22222222-2222-4222-8222-222222222222"
        );

        String raw = "  {\"message\":";

        var central = factory.create(
                eventId,
                raw,
                ParsingMode.CENTRAL
        );

        var agent = factory.create(
                central.eventId(),
                central.raw(),
                ParsingMode.AGENT
        );

        var centralAgain = factory.create(
                agent.eventId(),
                agent.raw(),
                ParsingMode.CENTRAL
        );

        assertAll(
                () -> assertEquals(eventId, central.eventId()),
                () -> assertEquals(eventId, agent.eventId()),
                () -> assertEquals(eventId, centralAgain.eventId()),
                () -> assertEquals(raw, central.raw()),
                () -> assertEquals(raw, agent.raw()),
                () -> assertEquals(raw, centralAgain.raw())
        );

        assertNull(central.parseStatus());
        assertNull(central.parsed());
        assertNull(central.parseError());

        assertEquals(ParseStatus.FAILED, agent.parseStatus());
        assertNull(agent.parsed());
        assertNotNull(agent.parseError());
        assertEquals("INVALID_JSON", agent.parseError().code());

        assertNull(centralAgain.parseStatus());
        assertNull(centralAgain.parsed());
        assertNull(centralAgain.parseError());
    }
}
