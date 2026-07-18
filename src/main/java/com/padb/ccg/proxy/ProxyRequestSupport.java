package com.padb.ccg.proxy;

import com.padb.ccg.core.exception.AuthPlatformUnavailableException;
import com.padb.ccg.core.exception.ProviderException;
import com.padb.ccg.core.exception.RateLimitExceededException;
import com.padb.ccg.core.exception.UnauthorizedException;
import com.padb.ccg.core.model.ProviderConfig;
import com.padb.ccg.core.model.RequestLogEntry;
import com.padb.ccg.core.spi.RateLimiter;
import com.padb.ccg.core.spi.RequestLogger;
import com.padb.ccg.auth.AuthService;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.server.ServerRequest;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.time.Instant;

/**
 * Anthropic / OpenAI 两个代理服务共享的请求处理支撑逻辑：
 * 认证 token 提取、模型提取、认证+限流、图片检测与剥离、请求日志、异常映射。
 */
@Component
public class ProxyRequestSupport {

    private static final Logger log = LoggerFactory.getLogger(ProxyRequestSupport.class);

    private final AuthService authService;
    private final RateLimiter rateLimiter;
    private final RequestLogger requestLogger;
    private final ObjectMapper objectMapper;

    public ProxyRequestSupport(AuthService authService, RateLimiter rateLimiter,
                               RequestLogger requestLogger, ObjectMapper objectMapper) {
        this.authService = authService;
        this.rateLimiter = rateLimiter;
        this.requestLogger = requestLogger;
        this.objectMapper = objectMapper;
    }

    /**
     * 从请求头提取认证 Token：优先 x-api-key，回退 Authorization: Bearer。
     */
    public static String extractAuthToken(ServerRequest request) {
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
     * 从请求 JSON 中提取顶层 model 字段值。
     * 用 Jackson 精确解析，避免字符串扫描命中 messages/metadata 里嵌套的 model 键。
     */
    public String extractModel(String body) {
        try {
            JsonNode model = objectMapper.readTree(body).get("model");
            return model != null && model.isTextual() ? model.asText() : null;
        } catch (Exception e) {
            log.warn("Failed to extract model from body", e);
            return null;
        }
    }

    /** 认证 → 限流，返回 personId。 */
    public Mono<String> authorizeAndRateLimit(String token, String model) {
        return authService.authorize(token, model)
                .flatMap(authResult -> {
                    String personId = authResult.personId();
                    return rateLimiter.tryAcquire(personId)
                            .thenReturn(personId);
                });
    }

    /**
     * 判断请求体是否含图片 content 块（Anthropic type=image 或 OpenAI type=image_url）。
     * 先做字符串预检避免每次请求都解析 JSON，命中预检再走 JSON 精确判断。
     */
    public boolean hasImageContent(String body) {
        if (body == null || (!body.contains("\"image\"") && !body.contains("\"image_url\""))) {
            return false;
        }
        try {
            JsonNode root = objectMapper.readTree(body);
            JsonNode messages = root.path("messages");
            if (!messages.isArray()) {
                return false;
            }
            for (JsonNode msg : messages) {
                JsonNode content = msg.path("content");
                if (!content.isArray()) {
                    continue;
                }
                for (JsonNode block : content) {
                    String t = block.path("type").asText();
                    if ("image".equals(t) || "image_url".equals(t)) {
                        return true;
                    }
                }
            }
        } catch (Exception e) {
            return false;
        }
        return false;
    }

    /**
     * 非视觉模型且请求体含图片块时剥离为占位文本；视觉模型或无图片时原样返回。
     * 剥离失败返回 null，由调用方转为 400 错误响应。
     */
    public String stripImagesIfNonVision(ProviderConfig mapping, String body, String model, String requestId) {
        if (mapping.supportsVision() || !hasImageContent(body)) {
            return body;
        }
        try {
            String stripped = AnthropicMessageImageStripper.stripImageBlocks(
                    objectMapper, body, AnthropicMessageImageStripper.DEFAULT_PLACEHOLDER);
            log.info("Stripped image blocks for non-vision model='{}' id={}", model, requestId);
            return stripped;
        } catch (Exception e) {
            log.warn("Failed to strip image blocks for model='{}' id={}: {}",
                    model, requestId, e.getMessage());
            return null;
        }
    }

    /**
     * 记录请求日志到数据库（fire-and-forget，不阻塞响应）
     */
    public void logRequest(String personId, ProviderConfig mapping, boolean success, String errorMsg,
                           int inputTokens, int outputTokens, Instant start) {
        int durationMs = (int) Duration.between(start, Instant.now()).toMillis();
        requestLogger.log(new RequestLogEntry(personId, mapping.modelName(), mapping.provider(),
                mapping.upstreamModelId(), success, errorMsg,
                inputTokens > 0 ? inputTokens : null,
                outputTokens > 0 ? outputTokens : null,
                durationMs, Instant.now()));
    }

    /** 异常映射结果：HTTP 状态码 + 错误类型 + 消息。 */
    public record MappedError(int status, String type, String message) {
    }

    /**
     * 将异常按根因类型映射为错误响应要素；无法识别返回 null，由调用方走 500 兜底。
     * 各服务用各自协议的错误格式渲染（Anthropic / OpenAI）。
     */
    public static MappedError mapError(Throwable e) {
        Throwable cause = e;
        while (cause.getCause() != null && cause != cause.getCause()) {
            cause = cause.getCause();
        }
        if (cause instanceof UnauthorizedException ue) {
            return new MappedError(403, "permission_error", ue.getMessage());
        }
        if (cause instanceof RateLimitExceededException rle) {
            return new MappedError(429, "rate_limit_error", rle.getMessage());
        }
        if (cause instanceof AuthPlatformUnavailableException apue) {
            return new MappedError(503, "api_error", apue.getMessage());
        }
        if (cause instanceof ProviderException pe) {
            return new MappedError(502, "api_error", pe.getMessage());
        }
        return null;
    }
}
