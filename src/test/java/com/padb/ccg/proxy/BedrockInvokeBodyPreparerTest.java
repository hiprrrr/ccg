package com.padb.ccg.proxy;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class BedrockInvokeBodyPreparerTest {

    private final ObjectMapper mapper = new ObjectMapper();

    @Test
    void sanitizeUserId_keepsSafeChars() {
        assertThat(BedrockInvokeBodyPreparer.sanitizeUserId("linhaibin930")).isEqualTo("linhaibin930");
        assertThat(BedrockInvokeBodyPreparer.sanitizeUserId("a.b-c:1_2")).isEqualTo("a.b-c:1_2");
    }

    @Test
    void sanitizeUserId_replacesUnsafeAndFallsBackWhenNoAlnum() {
        assertThat(BedrockInvokeBodyPreparer.sanitizeUserId("张三")).isEqualTo("gateway_user");
        assertThat(BedrockInvokeBodyPreparer.sanitizeUserId("user@host.com")).isEqualTo("user_host.com");
    }

    @Test
    void detectsOpenAiChatModelFamily() {
        assertThat(BedrockInvokeBodyPreparer.usesOpenAiChatShape("zai.glm-5")).isTrue();
        assertThat(BedrockInvokeBodyPreparer.usesOpenAiChatShape("deepseek.v3.2")).isTrue();
        assertThat(BedrockInvokeBodyPreparer.usesOpenAiChatShape("anthropic.claude-sonnet-4-6")).isFalse();
        assertThat(BedrockInvokeBodyPreparer.usesOpenAiChatShape(null)).isFalse();
    }

    @Test
    void normalizeMetadata_wrapsCustomToolsForZaiGlmWithGeoPrefix() throws Exception {
        // 跨区推理 ID（us.zai.*）：家族判定剥 geo 前缀，Z.ai 工具包装必须同样剥，两侧判定要同源
        String in = """
                {"max_tokens":10,"messages":[{"role":"user","content":"hi"}],
                 "tools":[{"name":"Read","description":"x",
                 "input_schema":{"type":"object","properties":{}}}]}""";
        String out = BedrockInvokeBodyPreparer.normalizeMetadata(mapper, in, "u1", "us.zai.glm-5");

        assertThat(out).contains("\"type\":\"function\"");
        assertThat(out).contains("\"function\":");
        assertThat(out).contains("\"name\":\"Read\"");
    }

    @Test
    void normalizeMetadata_addsCustomTypeWhenToolOmitsType() throws Exception {
        String in = """
                {"max_tokens":10,"messages":[{"role":"user","content":"hi"}],
                 "tools":[{"name":"Read","description":"x","input_schema":{"type":"object","properties":{}}}]}""";
        String out = BedrockInvokeBodyPreparer.normalizeMetadata(mapper, in, "u1");
        assertThat(out).contains("\"type\":\"custom\"");
    }

    @Test
    void normalizeMetadata_dropsHostedToolWithDateSuffix() throws Exception {
        String in = """
                {"max_tokens":10,"messages":[{"role":"user","content":"hi"}],
                 "tools":[{"type":"bash_20241022","name":"bash"}]}""";
        String out = BedrockInvokeBodyPreparer.normalizeMetadata(mapper, in, "u1");
        // Bedrock 不支持 Anthropic 托管工具类型，应被丢弃
        assertThat(out).doesNotContain("\"type\":\"bash_20241022\"");
        assertThat(out).contains("\"tools\":[]");
    }

    @Test
    void normalizeMetadata_wrapsCustomToolsForZaiGlm() throws Exception {
        String in = """
                {"max_tokens":10,"messages":[{"role":"user","content":"hi"}],
                 "tools":[{"name":"Read","description":"x",
                 "input_schema":{"type":"custom","properties":{"path":{"type":"string"}}}}]}""";
        String out = BedrockInvokeBodyPreparer.normalizeMetadata(mapper, in, "u1", "zai.glm-5");
        assertThat(out).contains("\"type\":\"function\"");
        assertThat(out).contains("\"function\":");
        assertThat(out).contains("\"name\":\"Read\"");
        assertThat(out).contains("\"parameters\":");
        assertThat(out).contains("\"type\":\"object\"");
        assertThat(out).doesNotContain("\"custom\":{\"name\":\"Read\"");
    }

    @Test
    void normalizeMetadata_dropsHostedToolsForZaiGlm() throws Exception {
        String in = """
                {"max_tokens":10,"messages":[{"role":"user","content":"hi"}],
                 "tools":[{"type":"text_editor_20250728","name":"str_replace_editor"}]}""";
        String out = BedrockInvokeBodyPreparer.normalizeMetadata(mapper, in, "u1", "zai.glm-5");
        // Bedrock 不支持 Anthropic 托管工具类型，即便走 zai.glm-5 也应被丢弃
        assertThat(out).doesNotContain("\"type\":\"text_editor_20250728\"");
        assertThat(out).contains("\"tools\":[]");
    }

    @Test
    void normalizeMetadata_overwritesClientMetadataWithGatewayUser() throws Exception {
        String in = """
                {"max_tokens":10,"messages":[{"role":"user","content":"hi"}],
                 "metadata":{"user_id":"/Users/x/.claude/sessions/foo"}}""";
        String out = BedrockInvokeBodyPreparer.normalizeMetadata(mapper, in, "linhaibin930");
        assertThat(out).contains("\"user_id\":\"linhaibin930\"");
        assertThat(out).doesNotContain("/Users/x");
    }

    @Test
    void normalizeMetadata_convertsAnthropicToolMessagesForZaiGlm() throws Exception {
        String in = """
                {"anthropic_version":"2023-06-01","system":[{"type":"text","text":"sys"}],
                 "stop_sequences":["END"],
                 "messages":[
                   {"role":"assistant","content":[{"type":"tool_use","id":"call_1","name":"Read","input":{"path":"pom.xml"}}]},
                   {"role":"user","content":[{"type":"tool_result","tool_use_id":"call_1","content":"ok"}]}
                 ]}""";
        String out = BedrockInvokeBodyPreparer.normalizeMetadata(mapper, in, "u1", "zai.glm-5");

        assertThat(out).contains("\"role\":\"system\",\"content\":\"sys\"");
        assertThat(out).contains("\"tool_calls\"");
        assertThat(out).contains("\"tool_call_id\":\"call_1\"");
        assertThat(out).contains("\"role\":\"tool\",\"tool_call_id\":\"call_1\",\"content\":\"ok\"");
        assertThat(out).contains("\"stop\":[\"END\"]");
        assertThat(out).doesNotContain("anthropic_version");
        assertThat(out).doesNotContain("stop_sequences");
    }

    @Test
    void normalizeMetadata_mapsAnthropicToolChoiceToOpenAiForZaiGlm() throws Exception {
        String in = """
                {"max_tokens":10,"tool_choice":{"type":"auto"},
                 "messages":[{"role":"user","content":"hi"}],
                 "tools":[{"name":"Read","description":"d","input_schema":{"type":"object","properties":{}}}]}""";
        String out = BedrockInvokeBodyPreparer.normalizeMetadata(mapper, in, "u1", "zai.glm-5");
        assertThat(out).contains("\"tool_choice\":\"auto\"");
    }

    @Test
    void normalizeMetadata_mapsAnthropicToolChoiceToolToOpenAiFunction() throws Exception {
        String in = """
                {"max_tokens":10,"tool_choice":{"type":"tool","name":"Read"},
                 "messages":[{"role":"user","content":"hi"}],
                 "tools":[{"name":"Read","input_schema":{"type":"object","properties":{}}}]}""";
        String out = BedrockInvokeBodyPreparer.normalizeMetadata(mapper, in, "u1", "zai.glm-5");
        assertThat(out).contains("\"tool_choice\":");
        assertThat(out).contains("\"type\":\"function\"");
        assertThat(out).contains("\"name\":\"Read\"");
    }

    @Test
    void normalizeMetadata_removesToolsWhenModelCapabilityDisablesTools() throws Exception {
        String in = """
                {"anthropic_version":"2023-06-01","max_tokens":10,"stream":true,"tool_choice":{"type":"auto"},
                 "system":[{"type":"text","text":"sys"}],
                 "messages":[{"role":"user","content":[{"type":"text","text":"hi"}]}],
                 "tools":[{"name":"Read","input_schema":{"type":"object","properties":{}}}]}""";
        String out = BedrockInvokeBodyPreparer.normalizeMetadata(mapper, in, "u1", "deepseek.v3.2",
                false, false);
        assertThat(out).doesNotContain("\"tools\"");
        assertThat(out).doesNotContain("\"tool_choice\"");
        assertThat(out).doesNotContain("\"stream\"");
        assertThat(out).doesNotContain("anthropic_version");
        assertThat(out).contains("\"role\":\"system\",\"content\":\"sys\"");
        assertThat(out).contains("\"role\":\"user\",\"content\":\"hi\"");
        assertThat(out).contains("\"messages\"");
    }

    @Test
    void normalizeMetadata_preservesOpenAiToolMessageToolCallId() throws Exception {
        String in = """
                {"max_tokens":10,"messages":[
                  {"role":"assistant","content":"","tool_calls":[{"id":"call_9","type":"function",
                    "function":{"name":"Read","arguments":"{}"}}]},
                  {"role":"tool","tool_call_id":"call_9","content":"file contents"}
                ]}""";
        String out = BedrockInvokeBodyPreparer.normalizeMetadata(mapper, in, "u1", "zai.glm-5");

        assertThat(out).contains("\"role\":\"tool\"");
        assertThat(out).contains("\"tool_call_id\":\"call_9\"");
        assertThat(out).contains("\"tool_calls\"");
    }

    @Test
    void normalizeMetadata_resolvesToolCallIdFromAlternateFields() throws Exception {
        String in = """
                {"messages":[
                  {"role":"assistant","content":[{"type":"tool_use","id":"call_alt","name":"Read","input":{}}]},
                  {"role":"user","content":[{"type":"tool_result","tool_call_id":"call_alt","content":"ok"}]}
                ]}""";
        String out = BedrockInvokeBodyPreparer.normalizeMetadata(mapper, in, "u1", "zai.glm-5");

        assertThat(out).contains("\"role\":\"tool\",\"tool_call_id\":\"call_alt\",\"content\":\"ok\"");
    }

    @Test
    void normalizeMetadata_dropsToolMessageWithoutToolCallId() throws Exception {
        String in = """
                {"max_tokens":10,"messages":[
                  {"role":"user","content":"hi"},
                  {"role":"tool","content":"orphan result"}
                ]}""";
        String out = BedrockInvokeBodyPreparer.normalizeMetadata(mapper, in, "u1", "zai.glm-5");

        assertThat(out).doesNotContain("\"role\":\"tool\"");
        assertThat(out).contains("\"role\":\"user\",\"content\":\"hi\"");
    }

    @Test
    void normalizeMetadata_stripsExperimentalAnthropicExtensionsForOpenAiShapeModels() throws Exception {
        String in = """
                {"anthropic_version":"2023-06-01","anthropic_beta":["tool-search-tool-2025-10-19"],
                 "betas":["x"],"thinking":{"type":"enabled"},"output_config":{"effort":"max"},
                 "mcp_servers":[],"service_tier":"auto","container":{"id":"c"},"context_management":{"edits":[]},
                 "messages":[{"role":"user","content":[{"type":"text","text":"hi","cache_control":{"type":"ephemeral"}}]}],
                 "tools":[{"name":"Read","custom":{"defer_loading":true},"cache_control":{"type":"ephemeral"},
                 "input_schema":{"type":"custom","properties":{"path":{"type":"string","input_examples":[]}}}}]}""";
        String out = BedrockInvokeBodyPreparer.normalizeMetadata(mapper, in, "u1", "zai.glm-5",
                true, true);

        assertThat(out).doesNotContain("anthropic_beta");
        assertThat(out).doesNotContain("\"betas\"");
        assertThat(out).doesNotContain("\"thinking\"");
        assertThat(out).doesNotContain("output_config");
        assertThat(out).doesNotContain("mcp_servers");
        assertThat(out).doesNotContain("service_tier");
        assertThat(out).doesNotContain("\"container\"");
        assertThat(out).doesNotContain("context_management");
        assertThat(out).doesNotContain("cache_control");
        assertThat(out).doesNotContain("defer_loading");
        assertThat(out).doesNotContain("input_examples");
        assertThat(out).contains("\"type\":\"function\"");
        assertThat(out).contains("\"name\":\"Read\"");
        assertThat(out).contains("\"parameters\":{\"type\":\"object\"");
    }

    @Test
    void normalizeMetadata_preservesToolResultImagesAsUserMessage() throws Exception {
        String in = """
                {"max_tokens":10,"messages":[
                  {"role":"assistant","content":[{"type":"tool_use","id":"call_1","name":"Read","input":{"path":"a.png"}}]},
                  {"role":"user","content":[{"type":"tool_result","tool_use_id":"call_1","content":[
                    {"type":"image","source":{"type":"base64","media_type":"image/png","data":"aGk="}},
                    {"type":"text","text":"a.png"}
                  ]}]}
                ]}""";
        String out = BedrockInvokeBodyPreparer.normalizeMetadata(mapper, in, "u1", "zai.glm-5");

        var messages = mapper.readTree(out).path("messages");
        assertThat(messages.get(0).path("role").asText()).isEqualTo("assistant");
        assertThat(messages.get(1).path("role").asText()).isEqualTo("tool");
        assertThat(messages.get(1).path("tool_call_id").asText()).isEqualTo("call_1");
        assertThat(messages.get(1).path("content").asText()).isEqualTo("a.png");

        var imageMsg = messages.get(2);
        assertThat(imageMsg.path("role").asText()).isEqualTo("user");
        var imageBlock = imageMsg.path("content").get(0);
        assertThat(imageBlock.path("type").asText()).isEqualTo("image_url");
        assertThat(imageBlock.path("image_url").path("url").asText())
                .isEqualTo("data:image/png;base64,aGk=");
    }

    @Test
    void normalizeMetadata_mergesToolResultImagesWithTrailingUserText() throws Exception {
        String in = """
                {"max_tokens":10,"messages":[
                  {"role":"assistant","content":[{"type":"tool_use","id":"call_1","name":"Read","input":{"path":"a.png"}}]},
                  {"role":"user","content":[
                    {"type":"tool_result","tool_use_id":"call_1","content":[
                      {"type":"image","source":{"type":"base64","media_type":"image/png","data":"aGk="}}
                    ]},
                    {"type":"text","text":"What color is it?"}
                  ]}
                ]}""";
        String out = BedrockInvokeBodyPreparer.normalizeMetadata(mapper, in, "u1", "zai.glm-5");

        var messages = mapper.readTree(out).path("messages");
        assertThat(messages.get(1).path("role").asText()).isEqualTo("tool");
        assertThat(messages.get(1).path("content").asText()).isEqualTo(" ");

        var userMsg = messages.get(2);
        assertThat(userMsg.path("role").asText()).isEqualTo("user");
        assertThat(userMsg.path("content").size()).isEqualTo(2);
        assertThat(userMsg.path("content").get(0).path("text").asText()).isEqualTo("What color is it?");
        assertThat(userMsg.path("content").get(1).path("type").asText()).isEqualTo("image_url");
    }

    @Test
    void normalizeMetadata_parsesStringifiedToolResultContentImages() throws Exception {
        String in = """
                {"max_tokens":10,"messages":[
                  {"role":"assistant","content":[{"type":"tool_use","id":"call_1","name":"Read","input":{"path":"a.png"}}]},
                  {"role":"user","content":[{"type":"tool_result","tool_use_id":"call_1",
                    "content":"[{\\"type\\":\\"image\\",\\"source\\":{\\"type\\":\\"base64\\",\\"media_type\\":\\"image/png\\",\\"data\\":\\"aGk=\\"}}]"}]}
                ]}""";
        String out = BedrockInvokeBodyPreparer.normalizeMetadata(mapper, in, "u1", "zai.glm-5");

        var messages = mapper.readTree(out).path("messages");
        assertThat(messages.get(1).path("role").asText()).isEqualTo("tool");
        var imageMsg = messages.get(2);
        assertThat(imageMsg.path("role").asText()).isEqualTo("user");
        assertThat(imageMsg.path("content").get(0).path("image_url").path("url").asText())
                .isEqualTo("data:image/png;base64,aGk=");
    }

    @Test
    void normalizeMetadata_preservesTopLevelImageInMessageWithToolResult() throws Exception {
        String in = """
                {"max_tokens":10,"messages":[
                  {"role":"assistant","content":[{"type":"tool_use","id":"call_1","name":"Read","input":{"path":"a.png"}}]},
                  {"role":"user","content":[
                    {"type":"tool_result","tool_use_id":"call_1","content":"ok"},
                    {"type":"image","source":{"type":"base64","media_type":"image/png","data":"aGk="}}
                  ]}
                ]}""";
        String out = BedrockInvokeBodyPreparer.normalizeMetadata(mapper, in, "u1", "zai.glm-5");

        var messages = mapper.readTree(out).path("messages");
        assertThat(messages.get(1).path("role").asText()).isEqualTo("tool");
        var imageMsg = messages.get(2);
        assertThat(imageMsg.path("role").asText()).isEqualTo("user");
        assertThat(imageMsg.path("content").get(0).path("type").asText()).isEqualTo("image_url");
    }
}
