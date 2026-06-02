package com.padb.ccg.server;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.reactive.function.server.RouterFunction;
import org.springframework.web.reactive.function.server.RouterFunctions;
import org.springframework.web.reactive.function.server.ServerResponse;

import static org.springframework.web.reactive.function.server.RequestPredicates.GET;
import static org.springframework.web.reactive.function.server.RequestPredicates.POST;

/**
 * WebFlux 路由器配置，定义 API 路径到处理器的映射
 */
@Configuration
public class GatewayRouter {

    /**
     * 注册路由：
     * - POST /v1/messages → 代理请求处理（Anthropic 格式）
     * - POST /v1/chat/completions → 代理请求处理（OpenAI 兼容格式）
     * - GET /health → 健康检查
     */
    @Bean
    public RouterFunction<ServerResponse> route(GatewayHandler handler) {
        return RouterFunctions
                .route(POST("/v1/messages"), handler::handle)
                .andRoute(POST("/v1/chat/completions"), handler::handleOpenAi)
                .andRoute(GET("/health"), handler::health);
    }
}
