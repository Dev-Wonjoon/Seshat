package com.seshat.repair;

import org.junit.jupiter.api.Test;
import tools.jackson.databind.json.JsonMapper;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class JsonRepairPolicyTest {

    private final JsonRepairPolicy policy = new JsonRepairPolicy(JsonMapper.builder().build());

    @Test
    void 한_곳의_문법만_수정하고_원문_토큰을_보존한다() {
        var missingComma = policy.assess("{\"a\":1 \"b\":2}");
        assertNull(missingComma.state());
        assertTrue(policy.preservesValues(missingComma, "{ \"a\": 1, \"b\": 2 }"));
        assertFalse(policy.preservesValues(missingComma, "{\"a\":1,\"b\":3}"));
        assertFalse(policy.preservesValues(missingComma, "{\"b\":2,\"a\":1}"));
        assertFalse(policy.preservesValues(missingComma, "{\"a\":1.0,\"b\":2}"));
        assertFalse(policy.preservesValues(missingComma, "{\"a\":1,\"b\":2,\"c\":null}"));

        var missingBrace = policy.assess("{\"a\":[{\"b\":\"text } ,\"}]");
        assertTrue(policy.preservesValues(missingBrace, "{\"a\":[{\"b\":\"text } ,\"}]}"));

        var trailingComma = policy.assess("{\"a\":{\"b\":\"\\u0062\",},\"b\":false}");
        assertTrue(policy.preservesValues(trailingComma, "{\"a\":{\"b\":\"\\u0062\"},\"b\":false}"));
        assertFalse(policy.preservesValues(trailingComma, "{\"a\":{\"b\":\"b\"},\"b\":false}"));
    }

    @Test
    void 빈_입력_평문_중복_키와_모호한_입력은_후보를_만들지_않는다() {
        assertEquals("EMPTY_INPUT", policy.assess(" \t").reason());
        assertEquals("NON_JSON_OBJECT", policy.assess("sshd[123]: text").reason());
        assertEquals("DUPLICATE_KEY", policy.assess("{\"a\":1,\"\\u0061\":1}").reason());
        assertEquals("ALREADY_VALID", policy.assess("{\"a\":{\"a\":1},\"b\":2}").reason());

        for (String raw : List.of(
                "{\"a\":", "{\"a\":\"abc", "{\"a\":01", "{\"a\":{\"b\":1",
                "{\"a\":1 \"b\":2,}", "{\"a\":1} {\"b\":2}", "{\"a\":1} garbage"
        )) {
            assertEquals("UNRECOVERABLE", policy.assess(raw).state(), raw);
        }
    }
}
