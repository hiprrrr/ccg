package com.padb.ccg.proxy;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.http.codec.ServerSentEvent;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class OpenAiResponsesRequestConverterTest {

    private ObjectMapper objectMapper;

    @BeforeEach
    void setUp() {
        objectMapper = new ObjectMapper();
    }

    @Test
    void toAnthropic_convertsSimpleStringInput() throws Exception {
        String body = """
                {"model":"glm-5","input":"hello","stream":true}
                """;

        String anthropic = OpenAiResponsesRequestConverter.toAnthropic(objectMapper, body);
        assertNotNull(anthropic);

        JsonNode root = objectMapper.readTree(anthropic);
        assertEquals("glm-5", root.path("model").asText());
        assertEquals("hello", root.path("messages").get(0).path("content").asText());
        assertTrue(root.path("stream").asBoolean());
        assertEquals(4096, root.path("max_tokens").asInt());
    }

    @Test
    void toAnthropic_convertsMessageArrayAndInstructions() throws Exception {
        String body = """
                {
                  "model":"glm-5",
                  "instructions":"be helpful",
                  "input":[
                    {"type":"message","role":"user","content":[{"type":"input_text","text":"ping"}]}
                  ],
                  "max_output_tokens": 1024
                }
                """;

        String anthropic = OpenAiResponsesRequestConverter.toAnthropic(objectMapper, body);
        JsonNode root = objectMapper.readTree(anthropic);

        assertEquals("be helpful", root.path("system").asText());
        assertEquals(1024, root.path("max_tokens").asInt());
        assertEquals("text", root.path("messages").get(0).path("content").get(0).path("type").asText());
        assertEquals("ping", root.path("messages").get(0).path("content").get(0).path("text").asText());
    }

    @Test
    void toAnthropic_convertsFunctionTools() throws Exception {
        String body = """
                {
                  "model":"glm-5",
                  "input":"hi",
                  "tools":[
                    {
                      "type":"function",
                      "name":"get_weather",
                      "description":"weather",
                      "parameters":{"type":"object","properties":{}}
                    }
                  ]
                }
                """;

        String anthropic = OpenAiResponsesRequestConverter.toAnthropic(objectMapper, body);
        JsonNode tool = objectMapper.readTree(anthropic).path("tools").get(0);
        assertEquals("get_weather", tool.path("name").asText());
        assertEquals("weather", tool.path("description").asText());
        assertEquals("object", tool.path("input_schema").path("type").asText());
    }
}
