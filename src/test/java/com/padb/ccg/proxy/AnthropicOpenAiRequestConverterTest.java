package com.padb.ccg.proxy;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class AnthropicOpenAiRequestConverterTest {

    private final ObjectMapper mapper = new ObjectMapper();

    @Test
    void shouldConvertAnthropicRequestToOpenAiChat() throws Exception {
        String anthropic = """
                {
                  "model": "glm-5",
                  "max_tokens": 1024,
                  "system": "You are helpful",
                  "messages": [{"role": "user", "content": "hello"}]
                }
                """;

        String openAi = AnthropicOpenAiRequestConverter.toOpenAiChat(mapper, anthropic, "glm-5.2", true);
        var root = mapper.readTree(openAi);

        assertEquals("glm-5.2", root.path("model").asText());
        assertTrue(root.path("stream").asBoolean());
        assertEquals(1024, root.path("max_tokens").asInt());
        assertEquals("system", root.path("messages").get(0).path("role").asText());
        assertEquals("You are helpful", root.path("messages").get(0).path("content").asText());
        assertEquals("user", root.path("messages").get(1).path("role").asText());
    }

    @Test
    void shouldConvertOpenAiResponseToAnthropicMessage() throws Exception {
        String openAi = """
                {
                  "id": "chatcmpl-1",
                  "choices": [{"message": {"role": "assistant", "content": "hi"}, "finish_reason": "stop"}],
                  "usage": {"prompt_tokens": 10, "completion_tokens": 2}
                }
                """;

        String anthropic = AnthropicOpenAiRequestConverter.toAnthropicMessage(mapper, openAi, "glm-5");
        var root = mapper.readTree(anthropic);

        assertEquals("message", root.path("type").asText());
        assertEquals("hi", root.path("content").get(0).path("text").asText());
        assertEquals(10, root.path("usage").path("input_tokens").asInt());
        assertEquals(2, root.path("usage").path("output_tokens").asInt());
    }

    @Test
    void shouldConvertAnthropicToolMessagesToOpenAiFormat() throws Exception {
        String anthropic = """
                {
                  "model": "glm-5.2",
                  "max_tokens": 1024,
                  "messages": [
                    {"role": "user", "content": "read pom"},
                    {"role": "assistant", "content": [
                      {"type": "tool_use", "id": "call_1", "name": "Read", "input": {"path": "pom.xml"}}
                    ]},
                    {"role": "user", "content": [
                      {"type": "tool_result", "tool_use_id": "call_1", "content": "ok"}
                    ]}
                  ]
                }
                """;

        String openAi = AnthropicOpenAiRequestConverter.toOpenAiChat(mapper, anthropic, "glm-5.2", true);
        var root = mapper.readTree(openAi);

        assertFalse(openAi.contains("tool_use"));
        assertFalse(openAi.contains("tool_result"));
        assertEquals("assistant", root.path("messages").get(1).path("role").asText());
        assertTrue(root.path("messages").get(1).has("tool_calls"));
        assertEquals("call_1", root.path("messages").get(1).path("tool_calls").get(0).path("id").asText());
        assertEquals("tool", root.path("messages").get(2).path("role").asText());
        assertEquals("call_1", root.path("messages").get(2).path("tool_call_id").asText());
        assertEquals("ok", root.path("messages").get(2).path("content").asText());
    }

    @Test
    void shouldOmitEmptyAssistantContentWhenOnlyToolCalls() throws Exception {
        String anthropic = """
                {
                  "model": "glm-5.2",
                  "messages": [
                    {"role": "user", "content": "go"},
                    {"role": "assistant", "content": [
                      {"type": "tool_use", "id": "call_1", "name": "Read", "input": {}}
                    ]}
                  ]
                }
                """;

        String openAi = AnthropicOpenAiRequestConverter.toOpenAiChat(mapper, anthropic, "glm-5.2", true);
        var assistant = mapper.readTree(openAi).path("messages").get(1);
        assertEquals("assistant", assistant.path("role").asText());
        assertTrue(assistant.has("tool_calls"));
        assertTrue(assistant.path("content").isMissingNode() || assistant.path("content").isNull());
    }

    @Test
    void shouldDropEmptyUserMessages() throws Exception {
        String anthropic = """
                {
                  "model": "glm-5.2",
                  "messages": [
                    {"role": "user", "content": ""},
                    {"role": "user", "content": "real"}
                  ]
                }
                """;

        String openAi = AnthropicOpenAiRequestConverter.toOpenAiChat(mapper, anthropic, "glm-5.2", false);
        var messages = mapper.readTree(openAi).path("messages");
        assertEquals(1, messages.size());
        assertEquals("real", messages.get(0).path("content").asText());
    }
}
