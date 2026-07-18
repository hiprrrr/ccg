package com.padb.ccg.proxy;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.padb.ccg.core.exception.ProviderException;
import com.padb.ccg.core.model.ProviderConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.web.reactive.function.client.ClientRequest;
import org.springframework.web.reactive.function.client.ClientResponse;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

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

    /** 构造带可编程 HTTP 应答的 handler，keysSeen 按顺序记录每次请求携带的 x-api-key */
    private HttpPassthroughProxyHandler handlerWithExchange(
            OtherProviderItem provider, List<String> keysSeen,
            java.util.function.Function<String, ClientResponse> responder) {
        OtherProvidersRegistry registry = mock(OtherProvidersRegistry.class);
        when(registry.require(provider.name())).thenReturn(provider);
        WebClient.Builder builder = WebClient.builder().exchangeFunction(request -> {
            keysSeen.add(request.headers().getFirst("x-api-key"));
            return Mono.just(responder.apply(request.headers().getFirst("x-api-key")));
        });
        return new HttpPassthroughProxyHandler(registry, new UpstreamProperties(3, 300), objectMapper, builder);
    }

    private static ClientResponse unauthorized() {
        return ClientResponse.create(HttpStatus.UNAUTHORIZED)
                .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
                .body("{\"error\":{\"message\":\"invalid api key\"}}")
                .build();
    }

    private static ClientResponse okSse() {
        String sse = """
                event: message_start
                data: {"type":"message_start","message":{"id":"m1","model":"m","usage":{"input_tokens":5,"output_tokens":0}}}

                event: message_stop
                data: {"type":"message_stop"}

                """;
        return ClientResponse.create(HttpStatus.OK)
                .header(HttpHeaders.CONTENT_TYPE, "text/event-stream")
                .body(sse)
                .build();
    }

    private static final String STREAM_BODY = """
            {"model":"m","stream":true,"messages":[{"role":"user","content":"hi"}]}
            """;

    @Test
    void shouldFailoverToAnotherKeyOn401() {
        var provider = new OtherProviderItem("p1", "http://upstream", List.of("key-a", "key-b"), "anthropic");
        List<String> keysSeen = new CopyOnWriteArrayList<>();
        HttpPassthroughProxyHandler h = handlerWithExchange(provider, keysSeen,
                key -> "key-a".equals(key) ? unauthorized() : okSse());
        var mapping = new ProviderConfig("p1", "m", "upstream-model", null, List.of("stream"));

        StepVerifier.create(h.forward(mapping, STREAM_BODY, new AtomicInteger(), new AtomicInteger(), "u1", "m", "t1"))
                .expectNextMatches(e -> e.data() != null && e.data().contains("message_start"))
                .thenConsumeWhile(e -> true)
                .verifyComplete();

        // key-a 只会出现在第一次（若被选中），故障转移后必须换 key-b
        assertThat(keysSeen).contains("key-b");
        assertThat(keysSeen.size()).isBetween(1, 2);
        if (keysSeen.size() == 2) {
            assertThat(keysSeen).containsExactly("key-a", "key-b");
        }
    }

    @Test
    void shouldTryEachKeyOnceWhenAllFail() {
        var provider = new OtherProviderItem("p1", "http://upstream", List.of("key-a", "key-b"), "anthropic");
        List<String> keysSeen = new CopyOnWriteArrayList<>();
        HttpPassthroughProxyHandler h = handlerWithExchange(provider, keysSeen, key -> unauthorized());
        var mapping = new ProviderConfig("p1", "m", "upstream-model", null, List.of("stream"));

        StepVerifier.create(h.forward(mapping, STREAM_BODY, new AtomicInteger(), new AtomicInteger(), "u1", "m", "t1"))
                .expectErrorMatches(e -> e instanceof ProviderException
                        && ((ProviderException) e).upstreamStatus() == 401)
                .verify();

        // 每个 key 恰好尝试一次
        assertThat(keysSeen).containsExactlyInAnyOrder("key-a", "key-b");
    }

    @Test
    void shouldNotFailoverOnNonAuthError() {
        var provider = new OtherProviderItem("p1", "http://upstream", List.of("key-a", "key-b"), "anthropic");
        List<String> keysSeen = new CopyOnWriteArrayList<>();
        HttpPassthroughProxyHandler h = handlerWithExchange(provider, keysSeen,
                key -> ClientResponse.create(HttpStatus.INTERNAL_SERVER_ERROR)
                        .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
                        .body("{\"error\":\"boom\"}")
                        .build());
        var mapping = new ProviderConfig("p1", "m", "upstream-model", null, List.of("stream"));

        StepVerifier.create(h.forward(mapping, STREAM_BODY, new AtomicInteger(), new AtomicInteger(), "u1", "m", "t1"))
                .expectErrorMatches(e -> e instanceof ProviderException
                        && ((ProviderException) e).upstreamStatus() == 500)
                .verify();

        // 500 不是鉴权/配额错误，不换 key 重试
        assertThat(keysSeen).hasSize(1);
    }
}

