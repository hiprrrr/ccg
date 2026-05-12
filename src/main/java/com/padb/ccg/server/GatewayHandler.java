package com.padb.ccg.server;

import com.padb.ccg.proxy.ProxyService;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.server.ServerRequest;
import org.springframework.web.reactive.function.server.ServerResponse;
import reactor.core.publisher.Mono;

/**
 * 网关 HTTP 请求处理器，将请求委托给 {@link ProxyService} 处理，
 * 并提供健康检查端点。
 */
@Component
public class GatewayHandler {

    private final ProxyService proxyService;

    public GatewayHandler(ProxyService proxyService) {
        this.proxyService = proxyService;
    }

    /**
     * 处理 /v1/messages 的代理请求
     *
     * @param request 服务端请求
     * @return 服务端响应 Mono
     */
    public Mono<ServerResponse> handle(ServerRequest request) {
        return proxyService.process(request);
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
