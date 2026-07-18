package com.padb.ccg.proxy;

import com.padb.ccg.routing.ProviderRegistryImpl;
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

    private final ProviderRegistryImpl providerRegistry;
    private final LlmUpstreamRouter upstreamRouter;
    private final ProxyRequestSupport support;
    private final ObjectMapper objectMapper;

    public ProxyService(ProviderRegistryImpl providerRegistry,
                        LlmUpstreamRouter upstreamRouter, ProxyRequestSupport support,
                        ObjectMapper objectMapper) {
        this.providerRegistry = providerRegistry;
        this.upstreamRouter = upstreamRouter;
        this.support = support;
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
        String token = ProxyRequestSupport.extractAuthToken(request);
        if (token == null || token.isBlank()) {
            return respondError(403, "permission_error", "Missing x-api-key or Authorization Bearer token");
        }

        return request.bodyToMono(String.class)
                .flatMap(body -> {
                    // 从请求体提取模型名称
                    String model = support.extractModel(body);
                    if (model == null) {
                        return respondError(400, "invalid_request_error", "Missing model in request body");
                    }
                    log.info("Gateway request accepted: id={} tokenHash={} model={} bodyChars={} toolUse={} toolResult={} tools={}",
                            requestId, token.hashCode(), model, body.length(),
                            body.contains("\"tool_use\""), body.contains("\"tool_result\""), body.contains("\"tools\""));

                    // 查找模型映射（aws / other-providers）
                    var mappingOpt = providerRegistry.resolve(model);
                    if (mappingOpt.isEmpty()) {
                        log.warn("No model mapping found for model='{}'", model);
                        return respondError(502, "api_error", "No model mapping configured for '" + model + "'");
                    }
                    var mapping = mappingOpt.get();

                    // 非视觉模型：剥离图片块为占位文本，避免长会话历史中残留 image 导致上游 400
                    String preparedBody = support.stripImagesIfNonVision(mapping, body, model, requestId);
                    if (preparedBody == null) {
                        return respondError(400, "invalid_request_error",
                                "Model '" + model + "' does not support image input");
                    }

                    // 归一化图片块：部分客户端（如 cc-switch）会把 OpenAI 风格的 image_url 块
                    // 发到 /v1/messages，这里统一转成 Anthropic 的 image 块，避免上游 400
                    final String normalizedBody = normalizeImageBlocks(preparedBody);

                    // 初始化 token 计数器和错误引用
                    var inputTokens = new AtomicInteger(0);
                    var outputTokens = new AtomicInteger(0);
                    var errorRef = new AtomicReference<String>(null);

                    // 认证 → 限流 → 代理转发
                    return support.authorizeAndRateLimit(token, model)
                            .flatMap(personId -> {
                                recordRequestContentPlaceholder(personId, model, body);
                                // 构建 SSE 流，附加错误捕获和日志记录
                                Flux<ServerSentEvent<String>> bedrockFlux = upstreamRouter
                                        .forward(mapping, normalizedBody, inputTokens, outputTokens, personId, model, requestId);
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
                                                    log.info("SSE response completed: id={} personId={} provider={} model={} durationMs={} inputTokens={} outputTokens={}",
                                                            requestId, personId, mapping.provider(), model,
                                                            Duration.between(start, Instant.now()).toMillis(),
                                                            inputTokens.get(), outputTokens.get());
                                                    support.logRequest(personId, mapping, true, null,
                                                            inputTokens.get(), outputTokens.get(), start);
                                                    break;
                                                case ON_ERROR:
                                                    log.warn("SSE response errored: id={} personId={} provider={} model={} durationMs={} error={}",
                                                            requestId, personId, mapping.provider(), model,
                                                            Duration.between(start, Instant.now()).toMillis(),
                                                            errorRef.get());
                                                    support.logRequest(personId, mapping, false, errorRef.get(),
                                                            inputTokens.get(), outputTokens.get(), start);
                                                    break;
                                                case CANCEL:
                                                    log.info("SSE response cancelled: id={} personId={} provider={} model={} durationMs={} inputTokens={} outputTokens={}",
                                                            requestId, personId, mapping.provider(), model,
                                                            Duration.between(start, Instant.now()).toMillis(),
                                                            inputTokens.get(), outputTokens.get());
                                                    if (inputTokens.get() > 0 || outputTokens.get() > 0) {
                                                        support.logRequest(personId, mapping, false, "client_cancelled",
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
     * 请求内容留存占位：后续如需保存每次请求内容，在这里接入脱敏、截断、加密和 1 个月清理策略。
     * 当前需求明确先不真正落库，避免提前保存可能包含敏感信息的 prompt 或代码内容。
     */
    private void recordRequestContentPlaceholder(String personId, String model, String body) {
        // Intentionally empty.
    }

    /**
     * 将请求体中 OpenAI 风格的 image_url content 块归一化为 Anthropic 的 image 块。
     * 不含 image_url 时原样返回，避免无谓的序列化开销。
     */
    private String normalizeImageBlocks(String body) {
        if (body == null || !body.contains("\"image_url\"")) {
            return body;
        }
        try {
            JsonNode root = objectMapper.readTree(body);
            if (!(root instanceof ObjectNode rootObj)) {
                return body;
            }
            JsonNode messages = rootObj.path("messages");
            if (!messages.isArray()) {
                return body;
            }
            boolean changed = false;
            for (JsonNode msg : messages) {
                if (!(msg instanceof ObjectNode msgObj)) {
                    continue;
                }
                JsonNode content = msgObj.path("content");
                if (!(content instanceof ArrayNode contentArr)) {
                    continue;
                }
                for (int i = 0; i < contentArr.size(); i++) {
                    JsonNode block = contentArr.get(i);
                    if ("image_url".equals(block.path("type").asText())) {
                        contentArr.set(i, convertImageUrlBlock(block));
                        changed = true;
                    }
                }
            }
            return changed ? objectMapper.writeValueAsString(rootObj) : body;
        } catch (Exception e) {
            log.warn("Failed to normalize image_url blocks: {}", e.getMessage());
            return body;
        }
    }

    /**
     * 单个 OpenAI image_url 块 → Anthropic image 块。
     * - data URI → source.type=base64（拆出 media_type 与 base64 data）
     * - http/https → source.type=url
     */
    private JsonNode convertImageUrlBlock(JsonNode block) {
        String url = block.path("image_url").path("url").asText("");
        int commaIdx = url.indexOf(',');
        if (url.startsWith("data:") && commaIdx > 0) {
            String header = url.substring(5, commaIdx);
            int semiIdx = header.indexOf(';');
            String mediaType = semiIdx > 0 ? header.substring(0, semiIdx) : header;
            String data = url.substring(commaIdx + 1);
            ObjectNode imageBlock = objectMapper.createObjectNode();
            imageBlock.put("type", "image");
            ObjectNode source = imageBlock.putObject("source");
            source.put("type", "base64");
            source.put("media_type", mediaType);
            source.put("data", data);
            return imageBlock;
        } else if (url.startsWith("http://") || url.startsWith("https://")) {
            ObjectNode imageBlock = objectMapper.createObjectNode();
            imageBlock.put("type", "image");
            ObjectNode source = imageBlock.putObject("source");
            source.put("type", "url");
            source.put("url", url);
            return imageBlock;
        }
        // 无法识别的 URL 格式：降级为占位文本，避免原样透传 image_url 导致上游 400
        ObjectNode textBlock = objectMapper.createObjectNode();
        textBlock.put("type", "text");
        textBlock.put("text", AnthropicMessageImageStripper.DEFAULT_PLACEHOLDER);
        return textBlock;
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
        ProxyRequestSupport.MappedError mapped = ProxyRequestSupport.mapError(e);
        if (mapped != null) {
            return respondError(mapped.status(), mapped.type(), mapped.message());
        }
        log.error("Unhandled error", e);
        return respondError(500, "api_error", "Internal gateway error");
    }
}
