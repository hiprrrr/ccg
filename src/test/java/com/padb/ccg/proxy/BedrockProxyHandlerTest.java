package com.padb.ccg.proxy;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.padb.ccg.core.model.ProviderConfig;
import org.junit.jupiter.api.Test;
import org.springframework.http.codec.ServerSentEvent;
import reactor.core.publisher.FluxSink;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.bedrockruntime.BedrockRuntimeAsyncClient;
import software.amazon.awssdk.services.bedrockruntime.model.InvokeModelRequest;
import software.amazon.awssdk.services.bedrockruntime.model.InvokeModelResponse;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

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

    /** 构造返回固定响应体的 mock Bedrock 客户端 */
    private BedrockRuntimeAsyncClient clientReturning(String responseBody) {
        BedrockRuntimeAsyncClient client = mock(BedrockRuntimeAsyncClient.class);
        InvokeModelResponse response = InvokeModelResponse.builder()
                .body(SdkBytes.fromString(responseBody, StandardCharsets.UTF_8))
                .build();
        when(client.invokeModel(any(InvokeModelRequest.class)))
                .thenReturn(CompletableFuture.completedFuture(response));
        return client;
    }

    @Test
    @SuppressWarnings("unchecked")
    void nonStreamingAccumulatesNovaUsage() {
        // Nova 非流式响应：usage 为驼峰字段，计账后请求日志才有真实 token 数
        String novaResponse = """
                {"output":{"message":{"role":"assistant","content":[{"text":"hi"}]}},
                 "stopReason":"end_turn","usage":{"inputTokens":11,"outputTokens":7}}""";
        var mapping = new ProviderConfig("aws", "nova-pro", "us.amazon.nova-pro-v1:0", null, List.of());
        var in = new AtomicInteger(0);
        var out = new AtomicInteger(0);
        FluxSink<ServerSentEvent<String>> sink = mock(FluxSink.class);

        handler.invokeNonStreaming(clientReturning(novaResponse), mapping, "{}", "u1", "nova-pro",
                BedrockModelFamily.NOVA, false, new AtomicReference<>(), sink,
                new AtomicReference<>(), new AtomicBoolean(), in, out, "t1");

        assertThat(in.get()).isEqualTo(11);
        assertThat(out.get()).isEqualTo(7);
        verify(sink, atLeastOnce()).next(any());
        verify(sink).complete();
    }

    @Test
    @SuppressWarnings("unchecked")
    void nonStreamingAccumulatesOpenAiUsage() {
        // OpenAI Chat 形态非流式响应：usage 为 prompt_tokens / completion_tokens
        String openAiResponse = """
                {"id":"x","choices":[{"index":0,"finish_reason":"stop",
                 "message":{"role":"assistant","content":"hi"}}],
                 "usage":{"prompt_tokens":5,"completion_tokens":9}}""";
        var mapping = new ProviderConfig("aws", "glm-5", "zai.glm-5", null, List.of());
        var in = new AtomicInteger(0);
        var out = new AtomicInteger(0);
        FluxSink<ServerSentEvent<String>> sink = mock(FluxSink.class);

        handler.invokeNonStreaming(clientReturning(openAiResponse), mapping, "{}", "u1", "glm-5",
                BedrockModelFamily.OPENAI_CHAT, true, new AtomicReference<>(), sink,
                new AtomicReference<>(), new AtomicBoolean(), in, out, "t1");

        assertThat(in.get()).isEqualTo(5);
        assertThat(out.get()).isEqualTo(9);
        verify(sink).complete();
    }
}
