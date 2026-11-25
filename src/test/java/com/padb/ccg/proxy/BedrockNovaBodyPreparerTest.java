package com.padb.ccg.proxy;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class BedrockNovaBodyPreparerTest {

    private final ObjectMapper mapper = new ObjectMapper();

    @Test
    void convertsTextOnlyRequest() throws Exception {
        String anthropic = """
                {"model":"m","max_tokens":512,"temperature":0.5,"top_p":0.9,
                 "stop_sequences":["END"],
                 "system":"You are helpful",
                 "messages":[{"role":"user","content":[{"type":"text","text":"hi"}]}]}
                """;
        JsonNode nova = mapper.readTree(BedrockNovaBodyPreparer.toNovaBody(mapper, anthropic));

        assertThat(nova.path("system").get(0).path("text").asText()).isEqualTo("You are helpful");
        assertThat(nova.path("messages").get(0).path("role").asText()).isEqualTo("user");
        assertThat(nova.path("messages").get(0).path("content").get(0).path("text").asText()).isEqualTo("hi");
        assertThat(nova.path("inferenceConfig").path("maxTokens").asInt()).isEqualTo(512);
        assertThat(nova.path("inferenceConfig").path("temperature").asDouble()).isEqualTo(0.5);
        assertThat(nova.path("inferenceConfig").path("topP").asDouble()).isEqualTo(0.9);
        assertThat(nova.path("inferenceConfig").path("stopSequences").get(0).asText()).isEqualTo("END");
        // Anthropic 专有字段不应出现在 Nova body 中
        assertThat(nova.has("model")).isFalse();
        assertThat(nova.has("metadata")).isFalse();
        assertThat(nova.has("anthropic_version")).isFalse();
        assertThat(nova.has("max_tokens")).isFalse();
    }

    @Test
    void convertsBase64ImageToNovaImageBlock() throws Exception {
        String anthropic = """
                {"model":"m","max_tokens":100,
                 "messages":[{"role":"user","content":[
                   {"type":"text","text":"看图"},
                   {"type":"image","source":{"type":"base64","media_type":"image/png","data":"aGk="}}
                 ]}]}
                """;
        JsonNode nova = mapper.readTree(BedrockNovaBodyPreparer.toNovaBody(mapper, anthropic));

        JsonNode imageBlock = nova.path("messages").get(0).path("content").get(1).path("image");
        assertThat(imageBlock.path("format").asText()).isEqualTo("png");
        assertThat(imageBlock.path("source").path("bytes").asText()).isEqualTo("aGk=");
    }

    @Test
    void replacesUrlImageWithPlaceholder() throws Exception {
        String anthropic = """
                {"model":"m","max_tokens":100,
                 "messages":[{"role":"user","content":[
                   {"type":"image","source":{"type":"url","url":"https://example.com/a.png"}}
                 ]}]}
                """;
        JsonNode nova = mapper.readTree(BedrockNovaBodyPreparer.toNovaBody(mapper, anthropic));

        JsonNode block = nova.path("messages").get(0).path("content").get(0);
        assertThat(block.has("image")).isFalse();
        assertThat(block.path("text").asText()).contains("Nova only accepts base64");
    }

    @Test
    void convertsToolUseAndToolResult() throws Exception {
        String anthropic = """
                {"model":"m","max_tokens":100,
                 "messages":[
                   {"role":"assistant","content":[
                     {"type":"tool_use","id":"toolu_1","name":"Read","input":{"path":"a.png"}}
                   ]},
                   {"role":"user","content":[
                     {"type":"tool_result","tool_use_id":"toolu_1","content":[
                       {"type":"text","text":"file content"}
                     ]}
                   ]}
                 ]}
                """;
        JsonNode nova = mapper.readTree(BedrockNovaBodyPreparer.toNovaBody(mapper, anthropic));

        JsonNode toolUse = nova.path("messages").get(0).path("content").get(0).path("toolUse");
        assertThat(toolUse.path("toolUseId").asText()).isEqualTo("toolu_1");
        assertThat(toolUse.path("name").asText()).isEqualTo("Read");
        assertThat(toolUse.path("input").path("path").asText()).isEqualTo("a.png");

        JsonNode toolResult = nova.path("messages").get(1).path("content").get(0).path("toolResult");
        assertThat(toolResult.path("toolUseId").asText()).isEqualTo("toolu_1");
        assertThat(toolResult.path("status").asText()).isEqualTo("success");
        assertThat(toolResult.path("content").get(0).path("text").asText()).isEqualTo("file content");
    }

    @Test
    void convertsToolsAndToolChoice() throws Exception {
        String anthropic = """
                {"model":"m","max_tokens":100,
                 "messages":[{"role":"user","content":"hi"}],
                 "tools":[{"name":"Read","description":"read file",
                           "input_schema":{"type":"object","properties":{"path":{"type":"string"}}}}],
                 "tool_choice":{"type":"auto"}}
                """;
        JsonNode nova = mapper.readTree(BedrockNovaBodyPreparer.toNovaBody(mapper, anthropic));

        JsonNode toolSpec = nova.path("toolConfig").path("tools").get(0).path("toolSpec");
        assertThat(toolSpec.path("name").asText()).isEqualTo("Read");
        assertThat(toolSpec.path("description").asText()).isEqualTo("read file");
        assertThat(toolSpec.path("inputSchema").path("json").path("type").asText()).isEqualTo("object");
        assertThat(nova.path("toolConfig").path("toolChoice").has("auto")).isTrue();
    }

    @Test
    void toolChoiceNoneOmitsToolConfig() throws Exception {
        String anthropic = """
                {"model":"m","max_tokens":100,
                 "messages":[{"role":"user","content":"hi"}],
                 "tools":[{"name":"Read","input_schema":{"type":"object","properties":{}}}],
                 "tool_choice":{"type":"none"}}
                """;
        JsonNode nova = mapper.readTree(BedrockNovaBodyPreparer.toNovaBody(mapper, anthropic));

        assertThat(nova.has("toolConfig")).isFalse();
    }

    @Test
    void toolChoiceToolMapsToNamedTool() throws Exception {
        String anthropic = """
                {"model":"m","max_tokens":100,
                 "messages":[{"role":"user","content":"hi"}],
                 "tools":[{"name":"Read","input_schema":{"type":"object","properties":{}}}],
                 "tool_choice":{"type":"tool","name":"Read"}}
                """;
        JsonNode nova = mapper.readTree(BedrockNovaBodyPreparer.toNovaBody(mapper, anthropic));

        assertThat(nova.path("toolConfig").path("toolChoice").path("tool").path("name").asText()).isEqualTo("Read");
    }

    @Test
    void replacesThinkingBlockWithPlaceholder() throws Exception {
        String anthropic = """
                {"model":"m","max_tokens":100,
                 "messages":[{"role":"assistant","content":[
                   {"type":"thinking","thinking":"let me think"},
                   {"type":"text","text":"answer"}
                 ]}]}
                """;
        JsonNode nova = mapper.readTree(BedrockNovaBodyPreparer.toNovaBody(mapper, anthropic));

        JsonNode blocks = nova.path("messages").get(0).path("content");
        assertThat(blocks.get(0).path("text").asText()).contains("thinking omitted");
        assertThat(blocks.get(1).path("text").asText()).isEqualTo("answer");
    }
}
