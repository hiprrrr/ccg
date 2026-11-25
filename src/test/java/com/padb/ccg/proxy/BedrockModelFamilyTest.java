package com.padb.ccg.proxy;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class BedrockModelFamilyTest {

    @Test
    void detectsAnthropicFamily() {
        assertThat(BedrockModelFamily.of("anthropic.claude-sonnet-4-6-v1:0")).isEqualTo(BedrockModelFamily.ANTHROPIC);
        assertThat(BedrockModelFamily.of("us.anthropic.claude-opus-4-7-v1:0")).isEqualTo(BedrockModelFamily.ANTHROPIC);
        assertThat(BedrockModelFamily.of("us-gov.anthropic.claude-3-5-sonnet-20240620-v1:0"))
                .isEqualTo(BedrockModelFamily.ANTHROPIC);
    }

    @Test
    void detectsOpenAiChatFamily() {
        for (String id : new String[]{
                "zai.glm-5", "deepseek.v3.2", "qwen.qwen3-vl-235b-a22b", "moonshotai.kimi-k3",
                "meta.llama3-3-70b-instruct-v1:0", "google.gemma-3-12b-it", "nvidia.nemotron-nano-12b-v2",
                "minimax.minimax-m2", "writer.palmyra-x5-v1:0", "openai.gpt-oss-120b-1:0",
                "mistral.mistral-large-3"}) {
            assertThat(BedrockModelFamily.of(id)).as(id).isEqualTo(BedrockModelFamily.OPENAI_CHAT);
        }
    }

    @Test
    void stripsGeoInferenceProfilePrefixes() {
        assertThat(BedrockModelFamily.of("us.moonshotai.kimi-k3")).isEqualTo(BedrockModelFamily.OPENAI_CHAT);
        assertThat(BedrockModelFamily.of("global.moonshotai.kimi-k3")).isEqualTo(BedrockModelFamily.OPENAI_CHAT);
        assertThat(BedrockModelFamily.of("eu.deepseek.v3.2")).isEqualTo(BedrockModelFamily.OPENAI_CHAT);
        assertThat(BedrockModelFamily.of("apac.qwen.qwen3-32b-v1:0")).isEqualTo(BedrockModelFamily.OPENAI_CHAT);
    }

    @Test
    void detectsNovaFamily() {
        assertThat(BedrockModelFamily.of("amazon.nova-lite-v1:0")).isEqualTo(BedrockModelFamily.NOVA);
        assertThat(BedrockModelFamily.of("amazon.nova-pro-v1:0")).isEqualTo(BedrockModelFamily.NOVA);
        assertThat(BedrockModelFamily.of("amazon.nova-2-lite-v1:0")).isEqualTo(BedrockModelFamily.NOVA);
        assertThat(BedrockModelFamily.of("us.amazon.nova-pro-v1:0")).isEqualTo(BedrockModelFamily.NOVA);
        assertThat(BedrockModelFamily.of("eu.amazon.nova-2-lite-v1:0")).isEqualTo(BedrockModelFamily.NOVA);
    }

    @Test
    void marksUnsupportedFamilies() {
        assertThat(BedrockModelFamily.of("cohere.command-r-v1:0")).isEqualTo(BedrockModelFamily.UNSUPPORTED);
        assertThat(BedrockModelFamily.of("ai21.jamba-1-5-large-v1:0")).isEqualTo(BedrockModelFamily.UNSUPPORTED);
        assertThat(BedrockModelFamily.of("us.xai.grok-4.7")).isEqualTo(BedrockModelFamily.UNSUPPORTED);
        assertThat(BedrockModelFamily.of("amazon.titan-text-express-v1:0")).isEqualTo(BedrockModelFamily.UNSUPPORTED);
        assertThat(BedrockModelFamily.of(null)).isEqualTo(BedrockModelFamily.UNSUPPORTED);
        assertThat(BedrockModelFamily.of("no-dot")).isEqualTo(BedrockModelFamily.UNSUPPORTED);
    }

    @Test
    void explicitFormatOverridesDetection() {
        assertThat(BedrockModelFamily.of("us.xai.grok-4.7", "openai")).isEqualTo(BedrockModelFamily.OPENAI_CHAT);
        assertThat(BedrockModelFamily.of("cohere.command-r-v1:0", "openai")).isEqualTo(BedrockModelFamily.OPENAI_CHAT);
        assertThat(BedrockModelFamily.of("zai.glm-5", "anthropic")).isEqualTo(BedrockModelFamily.ANTHROPIC);
        assertThat(BedrockModelFamily.of("qwen.qwen3-32b-v1:0", "nova")).isEqualTo(BedrockModelFamily.NOVA);
        // 空白配置回退到按 ID 推断
        assertThat(BedrockModelFamily.of("zai.glm-5", " ")).isEqualTo(BedrockModelFamily.OPENAI_CHAT);
        assertThat(BedrockModelFamily.of("zai.glm-5", null)).isEqualTo(BedrockModelFamily.OPENAI_CHAT);
        // 未知格式名视为不支持
        assertThat(BedrockModelFamily.of("zai.glm-5", "bogus")).isEqualTo(BedrockModelFamily.UNSUPPORTED);
    }
}
