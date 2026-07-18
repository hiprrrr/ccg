package com.padb.ccg.proxy;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 验证 OpenAI Chat Completions 请求体在转发 Bedrock glm-5 前，
 * 工具调用历史（tool_calls / tool_call_id）不会在格式转换中丢失。
 */
class OpenAiProxyServiceConversionTest {

    private final ObjectMapper mapper = new ObjectMapper();

    @Test
    void convertRequestBody_preservesToolCallHistoryForBedrockGlm() throws Exception {
        String openAi = """
                {"model":"glm-5","stream":true,"max_tokens":4096,
                 "messages":[
                   {"role":"user","content":"read file"},
                   {"role":"assistant","content":null,"tool_calls":[
                     {"id":"call_1","type":"function","function":{"name":"Read","arguments":"{\\"path\\":\\"pom.xml\\"}"}}
                   ]},
                   {"role":"tool","tool_call_id":"call_1","content":"file contents"}
                 ]}""";

        String anthropic = OpenAiChatRequestConverter.toAnthropic(mapper, openAi);
        assertThat(anthropic).contains("\"type\":\"tool_use\"");
        assertThat(anthropic).contains("\"tool_use_id\":\"call_1\"");
        assertThat(anthropic).doesNotContain("\"role\":\"tool\"");

        String bedrockBody = BedrockInvokeBodyPreparer.normalizeMetadata(
                mapper, anthropic, "u1", "zai.glm-5");

        assertThat(bedrockBody).contains("\"tool_call_id\":\"call_1\"");
        assertThat(bedrockBody).contains("\"tool_calls\"");
        assertThat(bedrockBody).contains("\"role\":\"tool\",\"tool_call_id\":\"call_1\"");
    }

    @Test
    void convertRequestBody_downgradesToolMessageWithoutToolCallId() throws Exception {
        String openAi = """
                {"model":"glm-5","messages":[
                  {"role":"user","content":"hi"},
                  {"role":"tool","content":"orphan result"}
                ]}""";

        String anthropic = OpenAiChatRequestConverter.toAnthropic(mapper, openAi);
        assertThat(anthropic).doesNotContain("\"role\":\"tool\"");
        assertThat(anthropic).contains("orphan result");

        String bedrockBody = BedrockInvokeBodyPreparer.normalizeMetadata(
                mapper, anthropic, "u1", "zai.glm-5");
        assertThat(bedrockBody).doesNotContain("\"role\":\"tool\"");
    }
}
