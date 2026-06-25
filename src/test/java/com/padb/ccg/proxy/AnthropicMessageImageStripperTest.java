package com.padb.ccg.proxy;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AnthropicMessageImageStripperTest {

    private final ObjectMapper mapper = new ObjectMapper();

    @Test
    void shouldStripImageUrlFromMessageContent() throws Exception {
        String in = """
                {"model":"glm-5.2","messages":[{"role":"user","content":[
                  {"type":"text","text":"hello"},
                  {"type":"image_url","image_url":{"url":"data:image/png;base64,abc"}}
                ]}]}
                """;
        String out = AnthropicMessageImageStripper.stripImageBlocks(mapper, in,
                AnthropicMessageImageStripper.DEFAULT_PLACEHOLDER);
        assertFalse(out.contains("\"image_url\""));
        assertTrue(out.contains(AnthropicMessageImageStripper.DEFAULT_PLACEHOLDER));
    }

    @Test
    void shouldStripAnthropicImageBlock() throws Exception {
        String in = """
                {"messages":[{"role":"user","content":[
                  {"type":"image","source":{"type":"base64","media_type":"image/png","data":"abc"}}
                ]}]}
                """;
        String out = AnthropicMessageImageStripper.stripImageBlocks(mapper, in, "[omitted]");
        assertFalse(out.contains("\"type\":\"image\""));
        assertTrue(out.contains("[omitted]"));
    }

    @Test
    void shouldReturnOriginalWhenNoImages() throws Exception {
        String in = "{\"messages\":[{\"role\":\"user\",\"content\":\"plain\"}]}";
        String out = AnthropicMessageImageStripper.stripImageBlocks(mapper, in, "[omitted]");
        assertTrue(out.contains("\"plain\""));
        assertFalse(out.contains("[omitted]"));
    }
}
