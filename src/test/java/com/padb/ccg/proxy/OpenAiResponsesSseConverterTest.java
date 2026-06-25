package com.padb.ccg.proxy;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.http.codec.ServerSentEvent;

import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class OpenAiResponsesSseConverterTest {

    private ObjectMapper objectMapper;
    private OpenAiResponsesSseConverter converter;

    @BeforeEach
    void setUp() {
        objectMapper = new ObjectMapper();
        converter = new OpenAiResponsesSseConverter(objectMapper, "glm-5");
    }

    @Test
    void convert_emitsLifecycleEventsForTextStream() throws Exception {
        List<String> types = collectEventTypes(converter.startStream());
        assertTrue(types.contains("response.created"));
        assertTrue(types.contains("response.in_progress"));

        String deltaEvent = """
                {"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"Hi"}}
                """;
        types.addAll(collectEventTypes(converter.convert(deltaEvent)));
        assertTrue(types.contains("response.output_text.delta"));

        String stopEvent = """
                {"type":"message_stop"}
                """;
        types.addAll(collectEventTypes(converter.convert(stopEvent)));
        assertTrue(types.contains("response.output_text.done"));
        assertTrue(types.contains("response.completed"));
    }

    @Test
    void endStreamIfOpen_emitsCompletedWhenUpstreamEndsEarly() {
        converter.startStream();
        List<String> types = collectEventTypes(converter.endStreamIfOpen());
        assertTrue(types.contains("response.completed"));
    }

    private List<String> collectEventTypes(List<ServerSentEvent<String>> events) {
        return events.stream()
                .map(ServerSentEvent::data)
                .map(this::readType)
                .collect(Collectors.toList());
    }

    private String readType(String json) {
        try {
            JsonNode node = objectMapper.readTree(json);
            return node.path("type").asText();
        } catch (Exception e) {
            return "";
        }
    }
}
