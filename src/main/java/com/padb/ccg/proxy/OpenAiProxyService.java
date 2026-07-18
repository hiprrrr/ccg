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
    private final OtherProvidersRegistry otherProvidersRegistry;
    private final RequestLogger requestLogger;
    private final ObjectMapper objectMapper;

    public OpenAiProxyService(AuthService authService, RateLimiter rateLimiter,
                              ProviderRegistryImpl providerRegistry,
                              LlmUpstreamRouter upstreamRouter, OtherProvidersRegistry otherProvidersRegistry,
                              RequestLogger requestLogger,
                              ObjectMapper objectMapper) {
        this.authService = authService;
        this.rateLimiter = rateLimiter;
        this.providerRegistry = providerRegistry;
        this.upstreamRouter = upstreamRouter;
        this.otherProvidersRegistry = otherProvidersRegistry;
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

                    log.info("OpenAI request accepted: id={} tokenHash={} model={} stream={} bodyChars={}",
                            requestId, token.hashCode(), model, streamRequested, body.length());

                    var mappingOpt = providerRegistry.resolve(model);
                    if (mappingOpt.isEmpty()) {
                        log.warn("No model mapping found for model='{}'", model);
                        return respondOpenAiError(404, "model_not_found", "Model '" + model + "' not found");
                    }
                    var mapping = mappingOpt.get();
                    boolean openAiPassthrough = isOpenAiPassthrough(mapping);

                    final String bodyToForward;
                    if (openAiPassthrough) {
                        bodyToForward = body;
                    } else {
                        String convertedBody = OpenAiChatRequestConverter.toAnthropic(objectMapper, body);
                        String anthropicBody = convertedBody != null ? convertedBody : body;
                        String stripped = stripImagesIfNonVision(mapping, anthropicBody, model, requestId);
                        if (stripped == null) {
                            return respondOpenAiError(400, "invalid_request_error",
                                    "Model '" + model + "' does not support image input");
                        }
                        bodyToForward = stripped;
                    }

                    // 初始化 token 计数器和错误引用
                    var inputTokens = new AtomicInteger(0);
                    var outputTokens = new AtomicInteger(0);
                    var errorRef = new AtomicReference<String>(null);

                    // 认证 → 限流 → 代理转发
                    return authorizeAndRateLimit(token, model)
                            .flatMap(personId -> {
                                if (openAiPassthrough) {
                                    if (streamRequested) {
                                        return handleOpenAiPassthroughStreamingRequest(mapping, bodyToForward, personId,
                                                model, requestId, inputTokens, outputTokens, errorRef, start);
                                    }
                                    return handleOpenAiPassthroughNonStreamingRequest(mapping, bodyToForward, personId,
                                            model, requestId, inputTokens, outputTokens, start);
                                }
                                if (streamRequested) {
                                    return handleStreamingRequest(mapping, bodyToForward, personId, model, requestId,
                                            inputTokens, outputTokens, errorRef, start);
                                }
                                return handleNonStreamingRequest(mapping, bodyToForward, personId, model, requestId,
                                        inputTokens, outputTokens, start);
                            });
                })
                .onErrorResume(e -> handleError(e));
    }

    /**
     * 处理 OpenAI Responses API（{@code POST /v1/responses}）代理请求。
     */
    public Mono<ServerResponse> processResponses(ServerRequest request) {
        Instant start = Instant.now();
        String requestId = UUID.randomUUID().toString().substring(0, 8);

        String token = extractAuthToken(request);
        if (token == null || token.isBlank()) {
            return respondOpenAiError(401, "invalid_request_error", "Missing API key");
        }

        return request.bodyToMono(String.class)
                .flatMap(body -> {
                    String model = extractModel(body);
                    if (model == null) {
                        return respondOpenAiError(400, "invalid_request_error", "Missing model in request body");
                    }

                    boolean streamRequested = isStreamRequested(body);
                    String convertedBody = OpenAiResponsesRequestConverter.toAnthropic(objectMapper, body);
                    if (convertedBody == null) {
                        return respondOpenAiError(400, "invalid_request_error", "Invalid Responses API request body");
                    }

                    log.info("OpenAI Responses request accepted: id={} tokenHash={} model={} stream={} bodyChars={}",
                            requestId, token.hashCode(), model, streamRequested, body.length());

                    var mappingOpt = providerRegistry.resolve(model);
                    if (mappingOpt.isEmpty()) {
                        log.warn("No model mapping found for model='{}'", model);
                        return respondOpenAiError(404, "model_not_found", "Model '" + model + "' not found");
                    }
                    var mapping = mappingOpt.get();

                    String stripped = stripImagesIfNonVision(mapping, convertedBody, model, requestId);
                    if (stripped == null) {
                        return respondOpenAiError(400, "invalid_request_error",
                                "Model '" + model + "' does not support image input");
                    }
                    final String bodyToForward = stripped;

                    var inputTokens = new AtomicInteger(0);
                    var outputTokens = new AtomicInteger(0);
                    var errorRef = new AtomicReference<String>(null);

                    return authorizeAndRateLimit(token, model)
                            .flatMap(personId -> {
                                if (streamRequested) {
                                    return handleResponsesStreamingRequest(mapping, bodyToForward, personId, model,
                                            requestId, inputTokens, outputTokens, errorRef, start);
                                }
                                return handleResponsesNonStreamingRequest(mapping, bodyToForward, personId, model,
                                        requestId, inputTokens, outputTokens, start);
                            });
                })
                .onErrorResume(e -> handleError(e));
    }

    /**
     * 处理 Responses API 流式请求。
     */
    private Mono<ServerResponse> handleResponsesStreamingRequest(ProviderConfig mapping, String body, String personId,
                                                                String model, String requestId,
                                                                AtomicInteger inputTokens, AtomicInteger outputTokens,
                                                                AtomicReference<String> errorRef, Instant start) {
        var converter = new OpenAiResponsesSseConverter(objectMapper, model);

        Flux<ServerSentEvent<String>> responsesFlux = upstreamRouter
                .forward(mapping, body, inputTokens, outputTokens, personId, model, requestId)
                .flatMap(event -> {
                    String data = event.data();
                    if (data == null || data.isEmpty()) {
                        return Flux.empty();
                    }
                    return Flux.fromIterable(converter.convert(data));
                })
                // 流正常结束后补齐未关闭的 response 结束事件
                .concatWith(Flux.defer(() -> Flux.fromIterable(converter.endStreamIfOpen())));

        return sseResponse(withStreamAccounting(withHeartbeat(responsesFlux),
                mapping, personId, model, requestId, inputTokens, outputTokens, errorRef, start));
    }

    /**
     * 处理 Responses API 非流式请求，返回完整 response 对象。
     */
    private Mono<ServerResponse> handleResponsesNonStreamingRequest(ProviderConfig mapping, String body, String personId,
                                                                     String model, String requestId,
                                                                     AtomicInteger inputTokens, AtomicInteger outputTokens,
                                                                     Instant start) {
        var converter = new OpenAiResponsesSseConverter(objectMapper, model);

        return upstreamRouter.forward(mapping, body, inputTokens, outputTokens, personId, model, requestId)
                .flatMap(event -> {
                    String data = event.data();
                    if (data == null || data.isEmpty()) {
                        return Flux.empty();
                    }
                    converter.convert(data);
                    return Flux.empty();
                })
                .then(Mono.defer(() -> {
                    converter.endStreamIfOpen();

                    ObjectNode response = objectMapper.createObjectNode();
                    response.put("id", converter.getResponseId());
                    response.put("object", "response");
                    response.put("created_at", System.currentTimeMillis() / 1000);
                    response.put("status", converter.getFinishStatus());
                    response.put("model", model);
                    response.putNull("error");
                    response.putNull("incomplete_details");

                    ArrayNode output = response.putArray("output");
                    if (!converter.getAccumulatedText().isEmpty()) {
                        ObjectNode message = output.addObject();
                        message.put("type", "message");
                        message.put("id", "msg_" + UUID.randomUUID().toString().substring(0, 8));
                        message.put("role", "assistant");
                        message.put("status", "completed");
                        ArrayNode content = message.putArray("content");
                        ObjectNode textPart = content.addObject();
                        textPart.put("type", "output_text");
                        textPart.put("text", converter.getAccumulatedText());
                        textPart.putArray("annotations");
                    }

                    ObjectNode usage = response.putObject("usage");
                    int inTok = converter.getInputTokens();
                    int outTok = converter.getOutputTokens();
                    usage.put("input_tokens", inTok);
                    usage.put("output_tokens", outTok);
                    usage.put("total_tokens", inTok + outTok);

                    log.info("OpenAI Responses non-stream completed: id={} personId={} model={} durationMs={} inputTokens={} outputTokens={}",
                            requestId, personId, model,
                            Duration.between(start, Instant.now()).toMillis(),
                            inTok, outTok);
                    logRequest(personId, mapping, true, null, inTok, outTok, start);

                    try {
                        return ServerResponse.ok()
                                .contentType(MediaType.APPLICATION_JSON)
                                .bodyValue(objectMapper.writeValueAsString(response));
                    } catch (Exception e) {
                        return respondOpenAiError(500, "internal_error", "Failed to serialize response");
                    }
                }));
    }

    /** other-providers 显式 openai 或未配置 api-format 时，OpenAI 客户端可透传上游协议。 */
    private boolean isOpenAiPassthrough(ProviderConfig mapping) {
        if (mapping.isAws()) {
            return false;
        }
        return otherProvidersRegistry.find(mapping.provider())
                .map(OtherProviderItem::supportsOpenAiClientPassthrough)
                .orElse(false);
    }

    /**
     * OpenAI 上游流式透传：不经 Anthropic 中转，直接转发 OpenAI SSE。
     */
    private Mono<ServerResponse> handleOpenAiPassthroughStreamingRequest(ProviderConfig mapping, String body,
                                                                     String personId, String model, String requestId,
                                                                     AtomicInteger inputTokens, AtomicInteger outputTokens,
                                                                     AtomicReference<String> errorRef, Instant start) {
        Flux<ServerSentEvent<String>> upstreamFlux = upstreamRouter
                .forwardOpenAi(mapping, body, inputTokens, outputTokens, personId, model, requestId);

        return sseResponse(withStreamAccounting(withHeartbeat(upstreamFlux),
                mapping, personId, model, requestId, inputTokens, outputTokens, errorRef, start));
    }

    /**
     * OpenAI 上游非流式透传：直接返回上游 JSON 响应。
     */
    private Mono<ServerResponse> handleOpenAiPassthroughNonStreamingRequest(ProviderConfig mapping, String body,
                                                                          String personId, String model,
                                                                          String requestId,
                                                                          AtomicInteger inputTokens,
                                                                          AtomicInteger outputTokens, Instant start) {
        return upstreamRouter.forwardOpenAi(mapping, body, inputTokens, outputTokens, personId, model, requestId)
                .next()
                .flatMap(event -> {
                    String json = event.data();
                    if (json == null || json.isBlank()) {
                        return respondOpenAiError(502, "api_error", "Empty response from upstream provider");
                    }
                    log.info("OpenAI non-stream passthrough completed: id={} personId={} model={} provider={} durationMs={} inputTokens={} outputTokens={}",
                            requestId, personId, model, mapping.provider(),
                            Duration.between(start, Instant.now()).toMillis(),
                            inputTokens.get(), outputTokens.get());
                    logRequest(personId, mapping, true, null,
                            inputTokens.get(), outputTokens.get(), start);
                    return ServerResponse.ok()
                            .contentType(MediaType.APPLICATION_JSON)
                            .bodyValue(json);
                })
                // 上游返回空响应时 next() 得到空 Mono，函数式端点会回 404，这里转为明确的 502
                .switchIfEmpty(respondOpenAiError(502, "api_error", "Empty response from upstream provider"));
    }

    /**
     * 处理流式请求
     */
    private Mono<ServerResponse> handleStreamingRequest(ProviderConfig mapping, String body, String personId,
                                                         String model, String requestId,
                                                         AtomicInteger inputTokens, AtomicInteger outputTokens,
                                                         AtomicReference<String> errorRef, Instant start) {
        var converter = new OpenAiSseConverter(objectMapper, model);

        // 将 Bedrock 的 Anthropic 格式 SSE 转换为 OpenAI 格式
        Flux<ServerSentEvent<String>> openAiFlux = upstreamRouter
                .forward(mapping, body, inputTokens, outputTokens, personId, model, requestId)
                .flatMap(event -> {
                    String data = event.data();
                    if (data == null || data.isEmpty()) {
                        return Flux.empty();
                    }
                    return Flux.fromIterable(converter.convert(data));
                });

        return sseResponse(withStreamAccounting(withHeartbeat(openAiFlux),
                mapping, personId, model, requestId, inputTokens, outputTokens, errorRef, start));
    }

    /**
     * 处理非流式请求（返回完整 JSON）
     */
    private Mono<ServerResponse> handleNonStreamingRequest(ProviderConfig mapping, String body, String personId,
                                                            String model, String requestId,
                                                            AtomicInteger inputTokens, AtomicInteger outputTokens,
                                                            Instant start) {
        var converter = new OpenAiSseConverter(objectMapper, model);
        var contentBuffer = new StringBuilder();
        var finishReasonRef = new AtomicReference<String>(null);

        // 收集所有内容并构建完整响应
        return upstreamRouter.forward(mapping, body, inputTokens, outputTokens, personId, model, requestId)
                .flatMap(event -> {
                    String data = event.data();
                    if (data == null || data.isEmpty()) {
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

    /** 认证 → 限流，返回 personId。 */
    private Mono<String> authorizeAndRateLimit(String token, String model) {
        return authService.authorize(token, model)
                .flatMap(authResult -> {
                    String personId = authResult.personId();
                    return rateLimiter.tryAcquire(personId)
                            .thenReturn(personId);
                });
    }

    /**
     * 非视觉模型且请求体含图片块时剥离为占位文本；视觉模型或无图片时原样返回。
     * 剥离失败返回 null，由调用方转为 400 错误响应。
     */
    private String stripImagesIfNonVision(ProviderConfig mapping, String body, String model, String requestId) {
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

    /** 合并 SSE 心跳：主流结束（或出错/取消）后心跳随之停止。 */
    private Flux<ServerSentEvent<String>> withHeartbeat(Flux<ServerSentEvent<String>> flux) {
        Flux<ServerSentEvent<String>> heartbeatFlux = Flux
                .interval(SSE_HEARTBEAT_INTERVAL)
                .map(i -> ServerSentEvent.<String>builder().comment("ping").build());
        return flux.publish(shared -> Flux.merge(
                shared,
                heartbeatFlux.takeUntilOther(shared.ignoreElements())
        ));
    }

    /** 流式结束统一记账：记录错误原因，完成/出错时落请求日志。 */
    private Flux<ServerSentEvent<String>> withStreamAccounting(Flux<ServerSentEvent<String>> flux,
                                                               ProviderConfig mapping, String personId,
                                                               String model, String requestId,
                                                               AtomicInteger inputTokens, AtomicInteger outputTokens,
                                                               AtomicReference<String> errorRef, Instant start) {
        return flux
                .doOnError(e -> {
                    String msg = e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName();
                    errorRef.set(msg);
                })
                .doFinally(signal -> {
                    switch (signal) {
                        case ON_COMPLETE -> {
                            log.info("OpenAI SSE completed: id={} personId={} model={} provider={} durationMs={} inputTokens={} outputTokens={}",
                                    requestId, personId, model, mapping.provider(),
                                    Duration.between(start, Instant.now()).toMillis(),
                                    inputTokens.get(), outputTokens.get());
                            logRequest(personId, mapping, true, null,
                                    inputTokens.get(), outputTokens.get(), start);
                        }
                        case ON_ERROR -> {
                            log.warn("OpenAI SSE errored: id={} personId={} model={} provider={} durationMs={} error={}",
                                    requestId, personId, model, mapping.provider(),
                                    Duration.between(start, Instant.now()).toMillis(),
                                    errorRef.get());
                            logRequest(personId, mapping, false, errorRef.get(),
                                    inputTokens.get(), outputTokens.get(), start);
                        }
                        case CANCEL -> log.info("OpenAI SSE cancelled: id={} personId={} model={} durationMs={}",
                                requestId, personId, model,
                                Duration.between(start, Instant.now()).toMillis());
                        default -> {
                        }
                    }
                });
    }

    /** 构建 SSE 流式响应。 */
    private Mono<ServerResponse> sseResponse(Flux<ServerSentEvent<String>> flux) {
        return ServerResponse.ok()
                .contentType(MediaType.TEXT_EVENT_STREAM)
                .header("Cache-Control", "no-cache")
                .header("Connection", "keep-alive")
                .body(BodyInserters.fromServerSentEvents(flux));
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
     * 从请求 JSON 中提取顶层 model 字段值。
     * 用 Jackson 精确解析，避免字符串扫描命中 messages/metadata 里嵌套的 model 键。
     */
    String extractModel(String body) {
        try {
            JsonNode model = objectMapper.readTree(body).get("model");
            return model != null && model.isTextual() ? model.asText() : null;
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
     * 判断 Anthropic 格式请求体是否含图片 content 块（type=image）。
     * 先做字符串预检避免每次请求都解析 JSON，命中预检再走 JSON 精确判断。
     */
    private boolean hasImageContent(String body) {
        if (body == null || !body.contains("\"image\"")) {
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
                    if ("image".equals(block.path("type").asText())) {
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
     * 构建 OpenAI 兼容的 JSON 错误响应
     */
    private Mono<ServerResponse> respondOpenAiError(int status, String type, String message) {
        // 直接用 Jackson 构建 JSON，避免 Map.of 不允许 null 值导致的 NPE
        ObjectNode error = objectMapper.createObjectNode();
        error.put("message", message);
        error.put("type", type);
        error.putNull("param");
        error.put("code", type);
        ObjectNode body = objectMapper.createObjectNode();
        body.set("error", error);
        try {
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
        if (cause instanceof com.padb.ccg.core.exception.AuthPlatformUnavailableException apue) {
            return respondOpenAiError(503, "api_error", apue.getMessage());
        }
        if (cause instanceof com.padb.ccg.core.exception.ProviderException pe) {
            return respondOpenAiError(502, "api_error", pe.getMessage());
        }
        log.error("Unhandled error in OpenAI proxy", e);
        return respondOpenAiError(500, "internal_error", "Internal gateway error");
    }
}
