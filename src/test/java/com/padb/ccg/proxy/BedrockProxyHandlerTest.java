package com.padb.ccg.proxy;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class BedrockProxyHandlerTest {

    private final BedrockProxyHandler handler = new BedrockProxyHandler(
            new BedrockProperties("us-east-1", null, null, null, false, 0),
            new UpstreamProperties(3, 300),
            new ObjectMapper());

    @Test
    void restoresEventNameFromChunkType() {
        var event = handler.toAnthropicSseEvent(
                "{\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"hi\"}}");

        assertThat(event).isNotNull();
        assertThat(event.event()).isEqualTo("content_block_delta");
        assertThat(event.data()).contains("text_delta");
    }

    @Test
    void skipsChunkWithoutEventType() {
        assertThat(handler.toAnthropicSseEvent("{\"amazon-bedrock-invocationMetrics\":{\"inputTokenCount\":3}}")).isNull();
    }

    @Test
    void skipsUnparseableChunk() {
        assertThat(handler.toAnthropicSseEvent("not json")).isNull();
        assertThat(handler.toAnthropicSseEvent(" ")).isNull();
    }
}
