package com.padb.ccg.proxy;

import com.padb.ccg.core.model.ProviderChannel;
import com.padb.ccg.core.model.ProviderConfig;
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
     * 转发请求到对应上游并返回 SSE 流。
     */
    public Flux<ServerSentEvent<String>> forward(ProviderConfig mapping, String requestBody,
                                                  AtomicInteger inputTokens, AtomicInteger outputTokens,
                                                  String username, String model, String traceId) {
        return switch (mapping.provider()) {
            case AWS -> bedrockHandler.forward(mapping, requestBody, inputTokens, outputTokens, username, model, traceId);
            case HUAWEI -> huaweiMaasHandler.forward(mapping, requestBody, inputTokens, outputTokens, username, model, traceId);
        };
    }
}
