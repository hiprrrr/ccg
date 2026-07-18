package com.padb.ccg.server;

import com.padb.ccg.core.exception.GatewayException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.annotation.Order;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.http.HttpStatus;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ResponseStatusException;
import org.springframework.web.server.ServerWebExchange;
import org.springframework.web.server.WebExceptionHandler;
import reactor.core.publisher.Mono;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.TimeoutException;

/**
 * 全局异常处理器，优先级设为 -2 以确保在其他异常处理器之前执行。
 * 将所有异常统一转换为 Anthropic API 兼容的 JSON 错误响应格式。
 */
@Component
@Order(-2)
public class GlobalErrorHandler implements WebExceptionHandler {

    private static final Logger log = LoggerFactory.getLogger(GlobalErrorHandler.class);

    private final ObjectMapper objectMapper;

    public GlobalErrorHandler(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    @Override
    public Mono<Void> handle(ServerWebExchange exchange, Throwable ex) {
        // 解包异常链，获取根因
        Throwable cause = ex;
        while (cause.getCause() != null && cause != cause.getCause()) {
            cause = cause.getCause();
        }

        HttpStatus status;
        String errorType;
        String message = cause.getMessage();

        // 根据异常类型映射 HTTP 状态码和 Anthropic 错误类型
        if (cause instanceof GatewayException ge) {
            status = HttpStatus.valueOf(ge.getHttpStatus());
            errorType = switch (ge.getHttpStatus()) {
                case 403 -> "permission_error";
                case 429 -> "rate_limit_error";
                default -> "api_error";
            };
        } else if (cause instanceof TimeoutException) {
            status = HttpStatus.GATEWAY_TIMEOUT;
            errorType = "timeout_error";
        } else if (cause instanceof ResponseStatusException rse) {
            HttpStatusCode code = rse.getStatusCode();
            if (code instanceof HttpStatus hs && hs.is4xxClientError()) {
                status = hs;
                errorType = "invalid_request_error";
            } else {
                status = HttpStatus.valueOf(code.value());
                errorType = "api_error";
            }
        } else {
            // 未预期的异常，记录完整堆栈
            log.error("Unhandled error in gateway", cause);
            status = HttpStatus.INTERNAL_SERVER_ERROR;
            errorType = "api_error";
            message = "Internal gateway error";
        }

        return writeErrorResponse(exchange, status, errorType, message);
    }

    /**
     * 写入 Anthropic 兼容的 JSON 错误响应
     * 格式: {"type": "error", "error": {"type": "...", "message": "..."}}
     */
    private Mono<Void> writeErrorResponse(ServerWebExchange exchange, HttpStatus status,
                                           String errorType, String message) {
        exchange.getResponse().setStatusCode(status);
        exchange.getResponse().getHeaders().setContentType(MediaType.APPLICATION_JSON);

        byte[] bytes;
        try {
            Map<String, Object> body = Map.of(
                    "type", "error",
                    "error", Map.of(
                            "type", errorType,
                            "message", message != null ? message : status.getReasonPhrase()
                    )
            );
            bytes = objectMapper.writeValueAsBytes(body);
        } catch (Exception e) {
            // JSON 序列化失败时的兜底响应
            bytes = "{\"type\":\"error\",\"error\":{\"type\":\"api_error\",\"message\":\"Internal error\"}}"
                    .getBytes(StandardCharsets.UTF_8);
        }

        DataBuffer buffer = exchange.getResponse().bufferFactory().wrap(bytes);
        return exchange.getResponse().writeWith(Mono.just(buffer));
    }
}
