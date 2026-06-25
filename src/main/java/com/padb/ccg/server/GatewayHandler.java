package com.padb.ccg.server;

import com.padb.ccg.proxy.OpenAiProxyService;
import com.padb.ccg.proxy.ProxyService;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.server.ServerRequest;
import org.springframework.web.reactive.function.server.ServerResponse;
import reactor.core.publisher.Mono;

/**
 * 网关 HTTP 请求处理器，将请求委托给相应的代理服务处理：
 * - /v1/messages → ProxyService（Anthropic 格式）
 * - /v1/chat/completions、/chat/completions → OpenAiProxyService（OpenAI Chat Completions 兼容格式）
 * - /v1/responses、/responses → OpenAiProxyService（OpenAI Responses API 格式）
 * 并提供健康检查端点。
 */
@Component
public class GatewayHandler {

    private final ProxyService proxyService;
    private final OpenAiProxyService openAiProxyService;

    public GatewayHandler(ProxyService proxyService, OpenAiProxyService openAiProxyService) {
        this.proxyService = proxyService;
        this.openAiProxyService = openAiProxyService;
    }

    /**
     * 处理 /v1/messages 的代理请求（Anthropic 格式）
     *
     * @param request 服务端请求
     * @return 服务端响应 Mono
     */
    public Mono<ServerResponse> handle(ServerRequest request) {
        return proxyService.process(request);
    }

    /**
     * 处理 /v1/chat/completions、/chat/completions 的代理请求（OpenAI Chat Completions 兼容格式）
     *
     * @param request 服务端请求
     * @return 服务端响应 Mono
     */
    public Mono<ServerResponse> handleOpenAi(ServerRequest request) {
        return openAiProxyService.process(request);
    }

    /**
     * 处理 /v1/responses、/responses 的代理请求（OpenAI Responses API 格式）
     *
     * @param request 服务端请求
     * @return 服务端响应 Mono
     */
    public Mono<ServerResponse> handleOpenAiResponses(ServerRequest request) {
        return openAiProxyService.processResponses(request);
    }

    /**
     * 健康检查端点，返回 "OK"
     *
     * @param request 服务端请求
     * @return 200 响应
     */
    public Mono<ServerResponse> health(ServerRequest request) {
        return ServerResponse.ok().bodyValue("OK");
    }
}
