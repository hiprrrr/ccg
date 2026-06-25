package com.padb.ccg.proxy;

import com.padb.ccg.core.model.ProviderChannel;
import com.padb.ccg.core.model.ProviderConfig;
import com.padb.ccg.core.exception.ProviderException;
import org.springframework.http.codec.ServerSentEvent;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * 按模型映射中的 {@link ProviderChannel} 将请求路由到 AWS Bedrock 或华为云 MaaS 处理器。
 */
@Component
public class LlmUpstreamRouter {

    private final BedrockProxyHandler bedrockHandler;
    private final HuaweiMaasProxyHandler huaweiMaasHandler;

    public LlmUpstreamRouter(BedrockProxyHandler bedrockHandler, HuaweiMaasProxyHandler huaweiMaasHandler) {
        this.bedrockHandler = bedrockHandler;
        this.huaweiMaasHandler = huaweiMaasHandler;
    }

    /**
     * 转发 Anthropic 格式请求到对应上游并返回 Anthropic SSE 流。
     */
    public Flux<ServerSentEvent<String>> forward(ProviderConfig mapping, String requestBody,
                                                  AtomicInteger inputTokens, AtomicInteger outputTokens,
                                                  String username, String model, String traceId) {
        return switch (mapping.provider()) {
            case AWS -> bedrockHandler.forward(mapping, requestBody, inputTokens, outputTokens, username, model, traceId);
            case HUAWEI -> huaweiMaasHandler.forward(mapping, requestBody, inputTokens, outputTokens, username, model, traceId);
        };
    }

    /**
     * 透传 OpenAI Chat Completions 到华为 MaaS（需 {@code huawei-maas.api-format=openai}）。
     */
    public Flux<ServerSentEvent<String>> forwardOpenAi(ProviderConfig mapping, String openAiBody,
                                                        AtomicInteger inputTokens, AtomicInteger outputTokens,
                                                        String username, String model, String traceId) {
        if (mapping.provider() != ProviderChannel.HUAWEI) {
            return Flux.error(new ProviderException("OpenAI upstream passthrough is only supported for Huawei MaaS"));
        }
        return huaweiMaasHandler.forwardOpenAi(
                mapping, openAiBody, inputTokens, outputTokens, username, model, traceId);
    }
}
