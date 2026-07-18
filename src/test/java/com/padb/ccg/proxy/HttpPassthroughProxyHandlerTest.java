package com.padb.ccg.proxy;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.web.reactive.function.client.WebClient;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

/**
 * 验证 HTTP 透传链路按模型视觉能力决定是否剥离图片块：
 * 视觉模型的 image 块必须原样保留，非视觉模型替换为占位文本。
 */
class HttpPassthroughProxyHandlerTest {

    private final ObjectMapper objectMapper = new ObjectMapper();
    private HttpPassthroughProxyHandler handler;

    @BeforeEach
    void setUp() {
        handler = new HttpPassthroughProxyHandler(
                mock(OtherProvidersRegistry.class),
                new UpstreamProperties(3, 300),
                objectMapper,
                WebClient.builder());
    }

    /** 含 Anthropic image 块的请求体 */
    private String bodyWithImage() {
        return """
                {"model":"claude-x","messages":[{"role":"user","content":[
                  {"type":"text","text":"看图"},
                  {"type":"image","source":{"type":"base64","media_type":"image/png","data":"QUJD"}}
                ]}]}
                """;
    }

    @Test
    void shouldKeepImageBlocksForVisionModel() throws Exception {
        String result = handler.prepareAnthropicRequestBody(bodyWithImage(), "upstream-model", true, true);

        JsonNode content = objectMapper.readTree(result).at("/messages/0/content");
        assertThat(content.get(1).path("type").asText()).isEqualTo("image");
        assertThat(content.get(1).at("/source/data").asText()).isEqualTo("QUJD");
    }

    @Test
    void shouldStripImageBlocksForNonVisionModel() throws Exception {
        String result = handler.prepareAnthropicRequestBody(bodyWithImage(), "upstream-model", true, false);

        JsonNode content = objectMapper.readTree(result).at("/messages/0/content");
        assertThat(content.get(1).path("type").asText()).isEqualTo("text");
        assertThat(content.get(1).path("text").asText())
                .isEqualTo(AnthropicMessageImageStripper.DEFAULT_PLACEHOLDER);
    }

    @Test
    void shouldReplaceModelAndStreamFlag() throws Exception {
        String result = handler.prepareAnthropicRequestBody(bodyWithImage(), "upstream-model", false, true);

        JsonNode root = objectMapper.readTree(result);
        assertThat(root.path("model").asText()).isEqualTo("upstream-model");
        assertThat(root.path("stream").asBoolean()).isFalse();
    }
}
