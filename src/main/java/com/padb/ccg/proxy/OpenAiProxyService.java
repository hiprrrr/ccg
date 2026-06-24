package com.padb.ccg.proxy;

import com.padb.ccg.core.model.ProviderConfig;
import com.padb.ccg.core.model.RequestLogEntry;
import com.padb.ccg.core.spi.RateLimiter;
import com.padb.ccg.core.spi.RequestLogger;
import com.padb.ccg.routing.ProviderRegistryImpl;
import com.padb.ccg.auth.AuthService;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
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
 * OpenAI 兼容格式的代理服务，编排完整的请求代理流程：
 * 1. 提取认证 Token（x-api-key 或 Authorization: Bearer）和请求模型
 * 2. 认证授权
 * 3. 限流检查
 * 4. 转发到 Bedrock（SSE 流式）
 * 5. 将 Anthropic 格式响应转换为 OpenAI 格式
 * 6. 记录请求日志（fire-and-forget）
 */
@Service
public class OpenAiProxyService {

    private static final Logger log = LoggerFactory.getLogger(OpenAiProxyService.class);
    private static final Duration SSE_HEARTBEAT_INTERVAL = Duration.ofSeconds(15);

    private final AuthService authService;
    private final RateLimiter rateLimiter;
    private final ProviderRegistryImpl providerRegistry;
    private final LlmUpstreamRouter upstreamRouter;
    private final RequestLogger requestLogger;
    private final ObjectMapper objectMapper;

    public OpenAiProxyService(AuthService authService, RateLimiter rateLimiter,
                              ProviderRegistryImpl providerRegistry,
                              LlmUpstreamRouter upstreamRouter, RequestLogger requestLogger,
                              ObjectMapper objectMapper) {
        this.authService = authService;
        this.rateLimiter = rateLimiter;
        this.providerRegistry = providerRegistry;
        this.upstreamRouter = upstreamRouter;
        this.requestLogger = requestLogger;
        this.objectMapper = objectMapper;
    }

    /**
     * 处理 OpenAI 兼容格式的代理请求
     */
    public Mono<ServerResponse> process(ServerRequest request) {
        Instant start = Instant.now();
        String requestId = UUID.randomUUID().toString().substring(0, 8);

        // 从请求头提取认证 Token
        String token = extractAuthToken(request);
        if (token == null || token.isBlank()) {
            return respondOpenAiError(401, "invalid_request_error", "Missing API key");
        }

        return request.bodyToMono(String.class)
                .flatMap(body -> {
                    // 从请求体提取模型名称
                    String model = extractModel(body);
                    if (model == null) {
                        return respondOpenAiError(400, "invalid_request_error", "Missing model in request body");
                    }

                    // 检测是否为流式请求
                    boolean streamRequested = isStreamRequested(body);

                    // 转换请求体（OpenAI → Anthropic 格式）
                    String convertedBody = convertRequestBody(body);
                    final String finalConvertedBody = convertedBody != null ? convertedBody : body;

                    log.info("OpenAI request accepted: id={} tokenHash={} model={} stream={} bodyChars={}",
                            requestId, token.hashCode(), model, streamRequested, body.length());

                    // 查找模型映射（AWS / 华为云）
                    var mappingOpt = providerRegistry.resolve(model);
                    if (mappingOpt.isEmpty()) {
                        log.warn("No model mapping found for model='{}'", model);
                        return respondOpenAiError(404, "model_not_found", "Model '" + model + "' not found");
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
                                return rateLimiter.tryAcquire(personId)
                                        .thenReturn(personId);
                            })
                            .flatMap(personId -> {
                                if (streamRequested) {
                                    return handleStreamingRequest(mapping, finalConvertedBody, personId, model, requestId,
                                            inputTokens, outputTokens, errorRef, start);
                                } else {
                                    return handleNonStreamingRequest(mapping, finalConvertedBody, personId, model, requestId,
                                            inputTokens, outputTokens, start);
                                }
                            });
                })
                .onErrorResume(e -> handleError(e));
    }

    /**
     * 处理流式请求
     */
    private Mono<ServerResponse> handleStreamingRequest(ProviderConfig mapping, String body, String personId,
                                                         String model, String requestId,
                                                         AtomicInteger inputTokens, AtomicInteger outputTokens,
                                                         AtomicReference<String> errorRef, Instant start) {
        var openAiConverterRef = new AtomicReference<OpenAiSseConverter>(null);

        // 初始化 OpenAI 转换器
        openAiConverterRef.set(new OpenAiSseConverter(objectMapper, model));

        // 获取 Bedrock 的 Anthropic 格式 SSE 流
        Flux<ServerSentEvent<String>> bedrockFlux = upstreamRouter
                .forward(mapping, body, inputTokens, outputTokens, personId, model, requestId);

        // 心跳流
        Flux<ServerSentEvent<String>> heartbeatFlux = Flux
                .interval(SSE_HEARTBEAT_INTERVAL)
                .map(i -> ServerSentEvent.<String>builder().comment("ping").build());

        // 转换为 OpenAI 格式
        Flux<ServerSentEvent<String>> openAiFlux = bedrockFlux
                .flatMap(event -> {
                    String data = event.data();
                    if (data == null || data.isEmpty()) {
                        return Flux.empty();
                    }

                    OpenAiSseConverter converter = openAiConverterRef.get();
                    if (converter == null) {
                        return Flux.empty();
                    }

                    // 将 Anthropic 格式转换为 OpenAI 格式
                    var converted = converter.convert(data);
                    return Flux.fromIterable(converted);
                })
                .publish(shared -> Flux.merge(
                        shared,
                        heartbeatFlux.takeUntilOther(shared.ignoreElements())
                ))
                .doOnError(e -> {
                    String msg = e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName();
                    errorRef.set(msg);
                })
                .doFinally(signal -> {
                    switch (signal) {
                        case ON_COMPLETE:
                            log.info("OpenAI SSE response completed: id={} personId={} model={} durationMs={} inputTokens={} outputTokens={}",
                                    requestId, personId, model,
                                    Duration.between(start, Instant.now()).toMillis(),
                                    inputTokens.get(), outputTokens.get());
                            logRequest(personId, mapping, true, null,
                                    inputTokens.get(), outputTokens.get(), start);
                            break;
                        case ON_ERROR:
                            log.warn("OpenAI SSE response errored: id={} personId={} model={} durationMs={} error={}",
                                    requestId, personId, model,
                                    Duration.between(start, Instant.now()).toMillis(),
                                    errorRef.get());
                            logRequest(personId, mapping, false, errorRef.get(),
                                    inputTokens.get(), outputTokens.get(), start);
                            break;
                        case CANCEL:
                            log.info("OpenAI SSE response cancelled: id={} personId={} model={} durationMs={}",
                                    requestId, personId, model,
                                    Duration.between(start, Instant.now()).toMillis());
                            break;
                        default:
                            break;
                    }
                });

        return ServerResponse.ok()
                .contentType(MediaType.TEXT_EVENT_STREAM)
                .header("Cache-Control", "no-cache")
                .header("Connection", "keep-alive")
                .body(BodyInserters.fromServerSentEvents(openAiFlux));
    }

    /**
     * 处理非流式请求（返回完整 JSON）
     */
    private Mono<ServerResponse> handleNonStreamingRequest(ProviderConfig mapping, String body, String personId,
                                                            String model, String requestId,
                                                            AtomicInteger inputTokens, AtomicInteger outputTokens,
                                                            Instant start) {
        var openAiConverterRef = new AtomicReference<OpenAiSseConverter>(null);
        var contentBuffer = new StringBuilder();
        var finishReasonRef = new AtomicReference<String>(null);

        // 初始化 OpenAI 转换器
        openAiConverterRef.set(new OpenAiSseConverter(objectMapper, model));

        // 获取 Bedrock 的 Anthropic 格式 SSE 流
        Flux<ServerSentEvent<String>> bedrockFlux = upstreamRouter
                .forward(mapping, body, inputTokens, outputTokens, personId, model, requestId);

        // 收集所有内容并构建完整响应
        return bedrockFlux
                .flatMap(event -> {
                    String data = event.data();
                    if (data == null || data.isEmpty()) {
                        return Flux.empty();
                    }

                    OpenAiSseConverter converter = openAiConverterRef.get();
                    if (converter == null) {
                        return Flux.empty();
                    }

                    // 转换并提取内容
                    var converted = converter.convert(data);
                    for (ServerSentEvent<String> sse : converted) {
                        String jsonData = sse.data();
                        if (jsonData != null && !jsonData.equals("[DONE]")) {
                            try {
                                JsonNode chunk = objectMapper.readTree(jsonData);
                                JsonNode choices = chunk.path("choices");
                                if (choices.isArray() && choices.size() > 0) {
                                    JsonNode delta = choices.get(0).path("delta");
                                    String content = delta.path("content").asText(null);
                                    if (content != null) {
                                        contentBuffer.append(content);
                                    }
                                    String finish = choices.get(0).path("finish_reason").asText(null);
                                    if (finish != null) {
                                        finishReasonRef.set(finish);
                                    }
                                }
                            } catch (Exception e) {
                                // ignore
                            }
                        }
                    }
                    return Flux.empty();
                })
                .then(Mono.defer(() -> {
                    // 构建完整的 OpenAI 响应
                    String responseId = "chatcmpl-" + UUID.randomUUID().toString().substring(0, 8);
                    ObjectNode response = objectMapper.createObjectNode();
                    response.put("id", responseId);
                    response.put("object", "chat.completion");
                    response.put("created", System.currentTimeMillis() / 1000);
                    response.put("model", model);

                    ArrayNode choices = response.putArray("choices");
                    ObjectNode choice = choices.addObject();
                    choice.put("index", 0);
                    ObjectNode message = choice.putObject("message");
                    message.put("role", "assistant");
                    message.put("content", contentBuffer.toString());
                    choice.put("finish_reason", finishReasonRef.get() != null ? finishReasonRef.get() : "stop");

                    ObjectNode usage = response.putObject("usage");
                    usage.put("prompt_tokens", inputTokens.get());
                    usage.put("completion_tokens", outputTokens.get());
                    usage.put("total_tokens", inputTokens.get() + outputTokens.get());

                    log.info("OpenAI non-stream response completed: id={} personId={} model={} durationMs={} inputTokens={} outputTokens={}",
                            requestId, personId, model,
                            Duration.between(start, Instant.now()).toMillis(),
                            inputTokens.get(), outputTokens.get());
                    logRequest(personId, mapping, true, null,
                            inputTokens.get(), outputTokens.get(), start);

                    try {
                        String jsonResponse = objectMapper.writeValueAsString(response);
                        return ServerResponse.ok()
                                .contentType(MediaType.APPLICATION_JSON)
                                .bodyValue(jsonResponse);
                    } catch (Exception e) {
                        return respondOpenAiError(500, "internal_error", "Failed to serialize response");
                    }
                }));
    }

    /**
     * 检测是否为流式请求
     */
    private boolean isStreamRequested(String body) {
        try {
            JsonNode root = objectMapper.readTree(body);
            if (root.has("stream")) {
                return root.get("stream").asBoolean(false);
            }
        } catch (Exception e) {
            // ignore
        }
        return false;
    }

    /**
     * 转换 OpenAI 请求体为 Anthropic 格式
     */
    private String convertRequestBody(String openAiBody) {
        try {
            JsonNode root = objectMapper.readTree(openAiBody);
            ObjectNode anthropic = objectMapper.createObjectNode();

            // 模型名称
            if (root.has("model")) {
                anthropic.put("model", root.get("model").asText());
            }

            // 消息转换
            if (root.has("messages")) {
                ArrayNode messages = anthropic.putArray("messages");
                for (JsonNode msg : root.get("messages")) {
                    ObjectNode convertedMsg = messages.addObject();
                    String role = msg.path("role").asText();
                    convertedMsg.put("role", role);

                    JsonNode content = msg.get("content");
                    if (content != null) {
                        if (content.isTextual()) {
                            convertedMsg.put("content", content.asText());
                        } else if (content.isArray()) {
                            convertedMsg.set("content", content);
                        }
                    }
                }
            }

            // max_tokens
            if (root.has("max_tokens")) {
                anthropic.put("max_tokens", root.get("max_tokens").asInt());
            } else {
                anthropic.put("max_tokens", 4096); // 默认值
            }

            // temperature
            if (root.has("temperature")) {
                anthropic.put("temperature", root.get("temperature").asDouble());
            }

            // top_p
            if (root.has("top_p")) {
                anthropic.put("top_p", root.get("top_p").asDouble());
            }

            // stop
            if (root.has("stop")) {
                anthropic.set("stop_sequences", root.get("stop"));
            }

            // stream
            if (root.has("stream")) {
                anthropic.put("stream", root.get("stream").asBoolean());
            }

            // tools (function calling)
            if (root.has("tools")) {
                ArrayNode tools = anthropic.putArray("tools");
                for (JsonNode tool : root.get("tools")) {
                    ObjectNode convertedTool = tools.addObject();
                    String toolType = tool.path("type").asText();
                    if ("function".equals(toolType)) {
                        convertedTool.put("name", tool.path("function").path("name").asText());
                        if (tool.path("function").has("description")) {
                            convertedTool.put("description", tool.path("function").path("description").asText());
                        }
                        if (tool.path("function").has("parameters")) {
                            convertedTool.set("input_schema", tool.path("function").get("parameters"));
                        }
                    }
                }
            }

            return objectMapper.writeValueAsString(anthropic);
        } catch (Exception e) {
            log.warn("Failed to convert OpenAI request to Anthropic format: {}", e.getMessage());
            return null;
        }
    }

    /**
     * 从请求头提取认证 Token
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
     * 从请求 JSON 中提取 model 字段值
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
     * 记录请求日志到数据库
     */
    private void logRequest(String personId, ProviderConfig mapping, boolean success, String errorMsg,
                            int inputTokens, int outputTokens, Instant start) {
        int durationMs = (int) Duration.between(start, Instant.now()).toMillis();
        requestLogger.log(new RequestLogEntry(personId, mapping.modelName(), mapping.provider(),
                mapping.upstreamModelId(), success, errorMsg,
                inputTokens > 0 ? inputTokens : null,
                outputTokens > 0 ? outputTokens : null,
                durationMs, Instant.now()));
    }

    /**
     * 构建 OpenAI 兼容的 JSON 错误响应
     */
    private Mono<ServerResponse> respondOpenAiError(int status, String type, String message) {
        try {
            Map<String, Object> body = Map.of(
                    "error", Map.of(
                            "message", message,
                            "type", type,
                            "param", (Object) null,
                            "code", type
                    )
            );
            byte[] bytes = objectMapper.writeValueAsBytes(body);
            return ServerResponse.status(status)
                    .header("Content-Type", "application/json")
                    .bodyValue(new String(bytes, StandardCharsets.UTF_8));
        } catch (Exception e) {
            return ServerResponse.status(500)
                    .header("Content-Type", "application/json")
                    .bodyValue("{\"error\":{\"message\":\"Internal error\",\"type\":\"internal_error\"}}");
        }
    }

    /**
     * 统一错误处理
     */
    private Mono<ServerResponse> handleError(Throwable e) {
        Throwable cause = e;
        while (cause.getCause() != null && cause != cause.getCause()) {
            cause = cause.getCause();
        }
        if (cause instanceof com.padb.ccg.core.exception.UnauthorizedException ue) {
            return respondOpenAiError(403, "permission_error", ue.getMessage());
        }
        if (cause instanceof com.padb.ccg.core.exception.RateLimitExceededException rle) {
            return respondOpenAiError(429, "rate_limit_error", rle.getMessage());
        }
        if (cause instanceof com.padb.ccg.core.exception.ProviderException pe) {
            return respondOpenAiError(502, "api_error", pe.getMessage());
        }
        log.error("Unhandled error in OpenAI proxy", e);
        return respondOpenAiError(500, "internal_error", "Internal gateway error");
    }
}
