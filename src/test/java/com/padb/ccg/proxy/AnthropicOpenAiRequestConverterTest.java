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

    @Test
    void shouldPreserveToolResultImagesAsUserMessage() throws Exception {
        String anthropic = """
                {
                  "model": "glm-5.2",
                  "max_tokens": 1024,
                  "messages": [
                    {"role": "user", "content": "read test.png"},
                    {"role": "assistant", "content": [
                      {"type": "tool_use", "id": "call_1", "name": "Read", "input": {"path": "test.png"}}
                    ]},
                    {"role": "user", "content": [
                      {"type": "tool_result", "tool_use_id": "call_1", "content": [
                        {"type": "image", "source": {"type": "base64", "media_type": "image/png", "data": "aGk="}},
                        {"type": "text", "text": "test.png"}
                      ]}
                    ]}
                  ]
                }
                """;

        String openAi = AnthropicOpenAiRequestConverter.toOpenAiChat(mapper, anthropic, "glm-5.2", true);
        var messages = mapper.readTree(openAi).path("messages");

        var toolMsg = messages.get(2);
        assertEquals("tool", toolMsg.path("role").asText());
        assertEquals("test.png", toolMsg.path("content").asText());

        var imageMsg = messages.get(3);
        assertEquals("user", imageMsg.path("role").asText());
        var imageBlock = imageMsg.path("content").get(0);
        assertEquals("image_url", imageBlock.path("type").asText());
        assertEquals("data:image/png;base64,aGk=", imageBlock.path("image_url").path("url").asText());
    }

    @Test
    void shouldMergeToolResultImagesWithTrailingUserText() throws Exception {
        String anthropic = """
                {
                  "model": "glm-5.2",
                  "max_tokens": 1024,
                  "messages": [
                    {"role": "assistant", "content": [
                      {"type": "tool_use", "id": "call_1", "name": "Read", "input": {"path": "test.png"}}
                    ]},
                    {"role": "user", "content": [
                      {"type": "tool_result", "tool_use_id": "call_1", "content": [
                        {"type": "image", "source": {"type": "base64", "media_type": "image/png", "data": "aGk="}}
                      ]},
                      {"type": "text", "text": "What color is it?"}
                    ]}
                  ]
                }
                """;

        String openAi = AnthropicOpenAiRequestConverter.toOpenAiChat(mapper, anthropic, "glm-5.2", true);
        var messages = mapper.readTree(openAi).path("messages");

        assertEquals("tool", messages.get(1).path("role").asText());
        assertEquals(" ", messages.get(1).path("content").asText());

        var userMsg = messages.get(2);
        assertEquals("user", userMsg.path("role").asText());
        assertEquals(2, userMsg.path("content").size());
        assertEquals("What color is it?", userMsg.path("content").get(0).path("text").asText());
        assertEquals("image_url", userMsg.path("content").get(1).path("type").asText());
    }

    @Test
    void shouldParseStringifiedToolResultContentImages() throws Exception {
        String anthropic = """
                {
                  "model": "glm-5.2",
                  "max_tokens": 1024,
                  "messages": [
                    {"role": "assistant", "content": [
                      {"type": "tool_use", "id": "call_1", "name": "Read", "input": {"path": "test.png"}}
                    ]},
                    {"role": "user", "content": [
                      {"type": "tool_result", "tool_use_id": "call_1",
                       "content": "[{\\"type\\":\\"image\\",\\"source\\":{\\"type\\":\\"base64\\",\\"media_type\\":\\"image/png\\",\\"data\\":\\"aGk=\\"}}]"}
                    ]}
                  ]
                }
                """;

        String openAi = AnthropicOpenAiRequestConverter.toOpenAiChat(mapper, anthropic, "glm-5.2", true);
        var messages = mapper.readTree(openAi).path("messages");

        assertEquals("tool", messages.get(1).path("role").asText());
        var imageMsg = messages.get(2);
        assertEquals("user", imageMsg.path("role").asText());
        assertEquals("data:image/png;base64,aGk=",
                imageMsg.path("content").get(0).path("image_url").path("url").asText());
    }
    @Test
    void shouldNotDuplicateBase64InStringifiedToolResultContent() throws Exception {
        // 字符串化的块数组解析成功后，tool 消息文本只取 text 块，base64 不能再进 tool 消息
        String anthropic = """
                {
                  "model": "glm-5.2",
                  "max_tokens": 1024,
                  "messages": [
                    {"role": "assistant", "content": [
                      {"type": "tool_use", "id": "call_1", "name": "Read", "input": {"path": "test.png"}}
                    ]},
                    {"role": "user", "content": [{"type": "tool_result", "tool_use_id": "call_1",
                     "content": "[{\\"type\\":\\"image\\",\\"source\\":{\\"type\\":\\"base64\\",\\"media_type\\":\\"image/png\\",\\"data\\":\\"aGk=\\"}},{\\"type\\":\\"text\\",\\"text\\":\\"a screenshot\\"}]"}]}
                  ]
                }
                """;

        String openAi = AnthropicOpenAiRequestConverter.toOpenAiChat(mapper, anthropic, "glm-5.2", true);
        var messages = mapper.readTree(openAi).path("messages");

        var toolMsg = messages.get(1);
        assertEquals("tool", toolMsg.path("role").asText());
        assertEquals("a screenshot", toolMsg.path("content").asText());
        // base64 只应出现在 user 消息的 image_url 块里，不能重复出现在 tool 消息文本中
        assertFalse(toolMsg.path("content").asText().contains("aGk="));
        var imageMsg = messages.get(2);
        assertEquals("data:image/png;base64,aGk=",
                imageMsg.path("content").get(0).path("image_url").path("url").asText());
    }

    @Test
    void shouldDegradeToolResultWithoutToolCallIdToUserText() throws Exception {
        // 缺 tool_use_id 的 tool_result 不能产出空 tool_call_id（OpenAI 上游会 400），降级为 user 文本
        String anthropic = """
                {
                  "model": "glm-5.2",
                  "max_tokens": 1024,
                  "messages": [
                    {"role": "assistant", "content": [
                      {"type": "tool_use", "id": "call_1", "name": "Read", "input": {"path": "test.png"}}
                    ]},
                    {"role": "user", "content": [
                      {"type": "tool_result", "content": "orphan result"}
                    ]}
                  ]
                }
                """;

        String openAi = AnthropicOpenAiRequestConverter.toOpenAiChat(mapper, anthropic, "glm-5.2", true);
        var messages = mapper.readTree(openAi).path("messages");

        // assistant 的 tool_use 消息 + 降级后的 user 文本消息，不应出现 role:tool
        assertEquals("assistant", messages.get(0).path("role").asText());
        assertEquals("user", messages.get(1).path("role").asText());
        assertEquals("orphan result", messages.get(1).path("content").asText());
        assertFalse(openAi.contains("\"role\":\"tool\""));
    }

    @Test
    void shouldResolveToolCallIdFromAlternateFields() throws Exception {
        // 兼容 tool_call_id / id 等替代字段命名，与 Bedrock 转换路径一致
        String anthropic = """
                {
                  "model": "glm-5.2",
                  "max_tokens": 1024,
                  "messages": [
                    {"role": "assistant", "content": [
                      {"type": "tool_use", "id": "call_alt", "name": "Read", "input": {"path": "test.png"}}
                    ]},
                    {"role": "user", "content": [{"type": "tool_result", "tool_call_id": "call_alt", "content": "ok"}]}
                  ]
                }
                """;

        String openAi = AnthropicOpenAiRequestConverter.toOpenAiChat(mapper, anthropic, "glm-5.2", true);
        var messages = mapper.readTree(openAi).path("messages");

        var toolMsg = messages.get(1);
        assertEquals("tool", toolMsg.path("role").asText());
        assertEquals("call_alt", toolMsg.path("tool_call_id").asText());
        assertEquals("ok", toolMsg.path("content").asText());
    }
}
