package com.padb.ccg.proxy;

import com.padb.ccg.core.model.ProviderConfig;
import com.padb.ccg.core.model.RequestLogEntry;
import com.padb.ccg.core.spi.RateLimiter;
import com.padb.ccg.core.spi.RequestLogger;
import com.padb.ccg.routing.ProviderRegistryImpl;
import com.padb.ccg.auth.AuthService;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.MediaType;
import org.springframework.http.codec.ServerSentEvent;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.BodyInserters;
import org.springframework.web.reactive.function.server.ServerRequest;
import org.springframework.web.reactive.function.server.ServerResponse;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * 代理服务，编排完整的请求代理流程：
 * 1. 提取认证 Token（x-api-key 或 Authorization: Bearer）和请求模型
 * 2. 认证授权
 * 3. 限流检查
 * 4. 转发到 Bedrock（SSE 流式）
 * 5. 记录请求日志（fire-and-forget）
 */
@Service
public class ProxyService {

    private static final Logger log = LoggerFactory.getLogger(ProxyService.class);
    private static final Duration SSE_HEARTBEAT_INTERVAL = Duration.ofSeconds(15);

    private final AuthService authService;
    private final RateLimiter rateLimiter;
    private final ProviderRegistryImpl providerRegistry;
    private final BedrockProxyHandler bedrockHandler;
    private final RequestLogger requestLogger;
    private final ObjectMapper objectMapper;

    public ProxyService(AuthService authService, RateLimiter rateLimiter,
                        ProviderRegistryImpl providerRegistry,
                        BedrockProxyHandler bedrockHandler, RequestLogger requestLogger,
                        ObjectMapper objectMapper) {
        this.authService = authService;
        this.rateLimiter = rateLimiter;
        this.providerRegistry = providerRegistry;
        this.bedrockHandler = bedrockHandler;
        this.requestLogger = requestLogger;
        this.objectMapper = objectMapper;
    }

    /**
     * 处理单次代理请求的完整生命周期
     *
     * @param request 服务端请求
     * @return SSE 流式响应 Mono
     */
    public Mono<ServerResponse> process(ServerRequest request) {
        Instant start = Instant.now();
        String requestId = UUID.randomUUID().toString().substring(0, 8);

        // 从请求头提取认证 Token：优先兼容 Anthropic 的 x-api-key，回退到 Claude Code 常用的 Bearer token
        String token = extractAuthToken(request);
        if (token == null || token.isBlank()) {
            return respondError(403, "permission_error", "Missing x-api-key or Authorization Bearer token");
        }

        return request.bodyToMono(String.class)
                .flatMap(body -> {
                    // 从请求体提取模型名称
                    String model = extractModel(body);
                    if (model == null) {
                        return respondError(400, "invalid_request_error", "Missing model in request body");
                    }
                    log.info("Gateway request accepted: id={} tokenHash={} model={} bodyChars={} toolUse={} toolResult={} tools={}",
                            requestId, token.hashCode(), model, body.length(),
                            body.contains("\"tool_use\""), body.contains("\"tool_result\""), body.contains("\"tools\""));

                    // 查找 Bedrock 模型映射
                    var mappingOpt = providerRegistry.resolve(model);
                    if (mappingOpt.isEmpty()) {
                        log.warn("No Bedrock mapping found for model='{}'", model);
                        return respondError(502, "api_error", "No Bedrock model mapping configured for '" + model + "'");
                    }
                    var mapping = mappingOpt.get();

                    // 初始化 token 计数器和错误引用
                    var inputTokens = new AtomicInteger(0);
                    var outputTokens = new AtomicInteger(0);
                    var errorRef = new AtomicReference<String>(null);

                    // 认证 → 限流 → 代理转发
                    return authService.authorize(token, model)
                            .flatMap(authResult -> {
                                String personId = authResult.personId();
                                recordRequestContentPlaceholder(personId, model, body);
                                return rateLimiter.tryAcquire(personId)
                                        .thenReturn(personId);
                            })
                            .flatMap(ok -> {
                                String personId = ok;
                                // 构建 SSE 流，附加错误捕获和日志记录
                                Flux<ServerSentEvent<String>> bedrockFlux = bedrockHandler
                                        .forward(mapping, body, inputTokens, outputTokens, personId, model, requestId);
                                Flux<ServerSentEvent<String>> heartbeatFlux = Flux
                                        .interval(SSE_HEARTBEAT_INTERVAL)
                                        // SSE 注释帧不会进入 Anthropic 事件流，但能保持连接活跃
                                        .map(i -> ServerSentEvent.<String>builder().comment("ping").build());
                                Flux<ServerSentEvent<String>> sseFlux = bedrockFlux
                                        .publish(shared -> Flux.merge(
                                                shared,
                                                heartbeatFlux.takeUntilOther(shared.ignoreElements())
                                        ))
                                        .doOnError(e -> {
                                            // 记录错误信息用于最终日志
                                            String msg = e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName();
                                            errorRef.set(msg);
                                        })
                                        .doFinally(signal -> {
                                            // 流结束时记录请求日志（fire-and-forget）
                                            switch (signal) {
                                                case ON_COMPLETE:
                                                    log.info("SSE response completed: id={} personId={} model={} durationMs={} inputTokens={} outputTokens={}",
                                                            requestId, personId, model,
                                                            Duration.between(start, Instant.now()).toMillis(),
                                                            inputTokens.get(), outputTokens.get());
                                                    logRequest(personId, model, mapping.bedrockModelId(),
                                                            true, null, inputTokens.get(), outputTokens.get(), start);
                                                    break;
                                                case ON_ERROR:
                                                    log.warn("SSE response errored: id={} personId={} model={} durationMs={} error={}",
                                                            requestId, personId, model,
                                                            Duration.between(start, Instant.now()).toMillis(),
                                                            errorRef.get());
                                                    logRequest(personId, model, mapping.bedrockModelId(),
                                                            false, errorRef.get(), inputTokens.get(), outputTokens.get(), start);
                                                    break;
                                                case CANCEL:
                                                    log.info("SSE response cancelled: id={} personId={} model={} durationMs={} inputTokens={} outputTokens={}",
                                                            requestId, personId, model,
                                                            Duration.between(start, Instant.now()).toMillis(),
                                                            inputTokens.get(), outputTokens.get());
                                                    if (inputTokens.get() > 0 || outputTokens.get() > 0) {
                                                        logRequest(personId, model, mapping.bedrockModelId(),
                                                                false, "client_cancelled",
                                                                inputTokens.get(), outputTokens.get(), start);
                                                    }
                                                    break;
                                                default:
                                                    break;
                                            }
                                        });

                                return ServerResponse.ok()
                                        .contentType(MediaType.TEXT_EVENT_STREAM)
                                        .body(BodyInserters.fromServerSentEvents(sseFlux));
                            });
                })
                .onErrorResume(e -> handleError(e));
    }

    /**
     * 从请求头提取认证 Token。Claude Code 使用 {@code ANTHROPIC_AUTH_TOKEN} 时会发送
     * {@code Authorization: Bearer <token>}。
     */
    private String extractAuthToken(ServerRequest request) {
        String apiKey = request.headers().firstHeader("x-api-key");
        if (apiKey != null && !apiKey.isBlank()) {
            return apiKey.trim();
        }
        String authorization = request.headers().firstHeader("Authorization");
        if (authorization == null || authorization.isBlank()) {
            return null;
        }
        String bearerPrefix = "Bearer ";
        if (authorization.regionMatches(true, 0, bearerPrefix, 0, bearerPrefix.length())) {
            String token = authorization.substring(bearerPrefix.length()).trim();
            return token.isBlank() ? null : token;
        }
        return null;
    }

    /**
     * 从请求 JSON 中提取 model 字段值（简单字符串解析，避免完整 JSON 反序列化）
     */
    private String extractModel(String body) {
        try {
            int idx = body.indexOf("\"model\"");
            if (idx < 0) return null;
            int colon = body.indexOf(":", idx);
            if (colon < 0) return null;
            int startQuote = body.indexOf("\"", colon + 1);
            if (startQuote < 0) return null;
            int endQuote = body.indexOf("\"", startQuote + 1);
            if (endQuote < 0) return null;
            return body.substring(startQuote + 1, endQuote);
        } catch (Exception e) {
            log.warn("Failed to extract model from body", e);
            return null;
        }
    }

    /**
     * 记录请求日志到数据库（fire-and-forget，不阻塞响应）
     */
    private void logRequest(String personId, String model, String bedrockModelId,
                            boolean success, String errorMsg,
                            int inputTokens, int outputTokens, Instant start) {
        int durationMs = (int) Duration.between(start, Instant.now()).toMillis();
        requestLogger.log(new RequestLogEntry(personId, model, bedrockModelId,
                success, errorMsg,
                inputTokens > 0 ? inputTokens : null,
                outputTokens > 0 ? outputTokens : null,
                durationMs, Instant.now()));
    }

    /**
     * 请求内容留存占位：后续如需保存每次请求内容，在这里接入脱敏、截断、加密和 1 个月清理策略。
     * 当前需求明确先不真正落库，避免提前保存可能包含敏感信息的 prompt 或代码内容。
     */
    private void recordRequestContentPlaceholder(String personId, String model, String body) {
        // Intentionally empty.
    }

    /**
     * 构建 Anthropic 兼容的 JSON 错误响应
     */
    private Mono<ServerResponse> respondError(int status, String type, String message) {
        try {
            Map<String, Object> body = Map.of(
                    "type", "error",
                    "error", Map.of("type", type, "message", message)
            );
            byte[] bytes = objectMapper.writeValueAsBytes(body);
            return ServerResponse.status(status)
                    .header("Content-Type", "application/json")
                    .bodyValue(new String(bytes, StandardCharsets.UTF_8));
        } catch (Exception e) {
            // JSON 序列化失败时的兜底响应
            return ServerResponse.status(500)
                    .header("Content-Type", "application/json")
                    .bodyValue("{\"type\":\"error\",\"error\":{\"type\":\"api_error\",\"message\":\"Internal error\"}}");
        }
    }

    /**
     * 统一错误处理：将异常按类型转换为对应的 HTTP 错误响应
     */
    private Mono<ServerResponse> handleError(Throwable e) {
        // 解包异常链，获取根因
        Throwable cause = e;
        while (cause.getCause() != null && cause != cause.getCause()) {
            cause = cause.getCause();
        }
        if (cause instanceof com.padb.ccg.core.exception.UnauthorizedException ue) {
            return respondError(403, "permission_error", ue.getMessage());
        }
        if (cause instanceof com.padb.ccg.core.exception.RateLimitExceededException rle) {
            return respondError(429, "rate_limit_error", rle.getMessage());
        }
        if (cause instanceof com.padb.ccg.core.exception.ProviderException pe) {
            return respondError(502, "api_error", pe.getMessage());
        }
        log.error("Unhandled error", e);
        return respondError(500, "api_error", "Internal gateway error");
    }
}
