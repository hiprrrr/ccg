package com.padb.ccg.proxy;

import com.padb.ccg.core.model.ProviderConfig;
import com.padb.ccg.core.exception.ProviderException;
import org.springframework.http.codec.ServerSentEvent;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * 按模型映射中的 provider 将请求路由到 AWS Bedrock 或 HTTP 透传（other-providers）处理器。
 */
@Component
public class LlmUpstreamRouter {

    private final BedrockProxyHandler bedrockHandler;
    private final HttpPassthroughProxyHandler httpPassthroughHandler;
    private final OtherProvidersRegistry otherProvidersRegistry;

    public LlmUpstreamRouter(BedrockProxyHandler bedrockHandler,
                             HttpPassthroughProxyHandler httpPassthroughHandler,
                             OtherProvidersRegistry otherProvidersRegistry) {
        this.bedrockHandler = bedrockHandler;
        this.httpPassthroughHandler = httpPassthroughHandler;
        this.otherProvidersRegistry = otherProvidersRegistry;
    }

    /**
     * 转发 Anthropic 格式请求到对应上游并返回 Anthropic SSE 流。
     */
    public Flux<ServerSentEvent<String>> forward(ProviderConfig mapping, String requestBody,
                                                  AtomicInteger inputTokens, AtomicInteger outputTokens,
                                                  String username, String model, String traceId) {
        if (mapping.isAws()) {
            return bedrockHandler.forward(mapping, requestBody, inputTokens, outputTokens, username, model, traceId);
        }
        return httpPassthroughHandler.forward(mapping, requestBody, inputTokens, outputTokens, username, model, traceId);
    }

    /**
     * 透传 OpenAI Chat Completions 到 other-providers（api-format=openai，或未配置 api-format）。
     */
    public Flux<ServerSentEvent<String>> forwardOpenAi(ProviderConfig mapping, String openAiBody,
                                                        AtomicInteger inputTokens, AtomicInteger outputTokens,
                                                        String username, String model, String traceId) {
        if (mapping.isAws()) {
            return Flux.error(new ProviderException("OpenAI upstream passthrough is only supported for other-providers"));
        }
        OtherProviderItem provider = otherProvidersRegistry.require(mapping.provider());
        if (!provider.supportsOpenAiClientPassthrough()) {
            return Flux.error(new ProviderException(
                    "OpenAI client passthrough requires api-format=openai or omit api-format for provider '"
                            + provider.name() + "'"));
        }
        return httpPassthroughHandler.forwardOpenAi(
                mapping, openAiBody, inputTokens, outputTokens, username, model, traceId);
    }
}
