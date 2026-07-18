package com.padb.ccg.proxy;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.padb.ccg.core.exception.ProviderException;
import com.padb.ccg.core.model.ProviderConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.codec.ServerSentEvent;
import org.springframework.http.client.reactive.ReactorClientHttpConnector;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.netty.http.client.HttpClient;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * HTTP 透传代理：按 {@code other-providers} 配置替换 base-url 与 api-key。
 * {@code api-format} 可选：{@code anthropic} / {@code openai}；未配置则按客户端协议透传，不做格式互转。
 */
@Component
public class HttpPassthroughProxyHandler {

    private static final Logger log = LoggerFactory.getLogger(HttpPassthroughProxyHandler.class);
    private static final String ANTHROPIC_VERSION = "2023-06-01";

    private final OtherProvidersRegistry otherProvidersRegistry;
    private final ObjectMapper objectMapper;
    private final WebClient webClient;

    public HttpPassthroughProxyHandler(OtherProvidersRegistry otherProvidersRegistry,
                                       UpstreamProperties upstreamProperties,
                                       ObjectMapper objectMapper, WebClient.Builder builder) {
        this.otherProvidersRegistry = otherProvidersRegistry;
        this.objectMapper = objectMapper;
        Duration timeout = Duration.ofSeconds(Math.max(1, upstreamProperties.timeoutSeconds()));
        HttpClient httpClient = HttpClient.create().responseTimeout(timeout);
        this.webClient = builder
                .clientConnector(new ReactorClientHttpConnector(httpClient))
                .defaultHeader(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
                .build();
    }

    /**
     * 转发 Anthropic 格式请求，返回 Anthropic SSE（供 {@code /v1/messages} 及 Bedrock 风格链路使用）。
     */
    public Flux<ServerSentEvent<String>> forward(ProviderConfig mapping, String requestBody,
                                                  AtomicInteger inputTokens, AtomicInteger outputTokens,
                                                  String username, String model, String traceId) {
        OtherProviderItem provider = otherProvidersRegistry.require(mapping.provider());
        UpstreamAccountSelector.AccountSelection account = UpstreamAccountSelector.select(provider);
        final String correlationId = normalizeTraceId(traceId);
        // 优先尊重请求体 stream；未声明时再回退到 capabilities 中的 stream/streaming
        boolean streaming = resolveStreaming(mapping, requestBody);

        // 仅显式 api-format=openai 时，将 Anthropic 客户端请求转到 OpenAI 上游并做协议转换；
        // anthropic 或未配置（透传）则按 Anthropic 协议直连上游。
        if (provider.isOpenAiFormat()) {
            return forwardAnthropicViaOpenAiUpstream(provider, mapping, requestBody, inputTokens, outputTokens,
                    username, model, correlationId, streaming, account);
        }
        return forwardAnthropicUpstream(provider, mapping, requestBody, inputTokens, outputTokens,
                username, model, correlationId, streaming, account);
    }

    /**
     * 透传 OpenAI Chat Completions（api-format=openai，或未配置 api-format 的透传模式）。
     */
    public Flux<ServerSentEvent<String>> forwardOpenAi(ProviderConfig mapping, String openAiBody,
                                                        AtomicInteger inputTokens, AtomicInteger outputTokens,
                                                        String username, String model, String traceId) {
        OtherProviderItem provider = otherProvidersRegistry.require(mapping.provider());
        if (!provider.supportsOpenAiClientPassthrough()) {
            return Flux.error(new ProviderException(
                    "OpenAI client passthrough requires api-format=openai or omit api-format for provider '"
                            + provider.name() + "' (current=" + provider.resolvedApiFormat() + ")"));
        }

        UpstreamAccountSelector.AccountSelection account = UpstreamAccountSelector.select(provider);
        final String correlationId = normalizeTraceId(traceId);
        boolean streaming = isStreamRequested(openAiBody);
        String body;
        try {
            body = prepareOpenAiRequestBody(openAiBody, mapping.upstreamModelId(), streaming);
        } catch (JsonProcessingException e) {
            return Flux.error(new ProviderException("Invalid OpenAI request for provider '"
                    + provider.name() + "': " + e.getMessage()));
        }

        log.info("HTTP passthrough OpenAI: provider={} traceId={} user={} userModel={} upstreamModel={} streaming={}",
                provider.name(), correlationId, username, model, mapping.upstreamModelId(), streaming);

        if (streaming) {
            return invokeOpenAiStreaming(provider, body, account, inputTokens, outputTokens,
                    correlationId, username, model, false);
        }
        return invokeOpenAiNonStreamingAsSingleEvent(provider, body, account, inputTokens, outputTokens,
                correlationId, username, model);
    }

    private Flux<ServerSentEvent<String>> forwardAnthropicUpstream(OtherProviderItem provider,
                                                                   ProviderConfig mapping, String requestBody,
                                                                   AtomicInteger inputTokens, AtomicInteger outputTokens,
                                                                   String username, String model, String traceId,
                                                                   boolean streaming,
                                                                   UpstreamAccountSelector.AccountSelection account) {
        String body;
        try {
            body = prepareAnthropicRequestBody(requestBody, mapping.upstreamModelId(), streaming,
                    mapping.supportsVision());
        } catch (JsonProcessingException e) {
            return Flux.error(new ProviderException("Invalid request body for provider '"
                    + provider.name() + "': " + e.getMessage()));
        }

        log.info("HTTP passthrough Anthropic: provider={} traceId={} user={} userModel={} upstreamModel={} streaming={}",
                provider.name(), traceId, username, model, mapping.upstreamModelId(), streaming);

        if (streaming) {
            return invokeAnthropicStreaming(provider, body, account, inputTokens, outputTokens, traceId, username, model);
        }
        return invokeAnthropicNonStreaming(provider, body, account, model, inputTokens, outputTokens, traceId, username);
    }

    private Flux<ServerSentEvent<String>> forwardAnthropicViaOpenAiUpstream(OtherProviderItem provider,
                                                                             ProviderConfig mapping, String requestBody,
                                                                             AtomicInteger inputTokens,
                                                                             AtomicInteger outputTokens,
                                                                             String username, String model,
                                                                             String traceId, boolean streaming,
                                                                             UpstreamAccountSelector.AccountSelection account) {
        String openAiBody;
        try {
            openAiBody = AnthropicOpenAiRequestConverter.toOpenAiChat(
                    objectMapper, requestBody, mapping.upstreamModelId(), streaming);
        } catch (JsonProcessingException e) {
            return Flux.error(new ProviderException("Failed to convert Anthropic request for provider '"
                    + provider.name() + "' OpenAI API: " + e.getMessage()));
        }

        log.info("HTTP passthrough OpenAI upstream (Anthropic client): provider={} traceId={} user={} userModel={} upstreamModel={} streaming={}",
                provider.name(), traceId, username, model, mapping.upstreamModelId(), streaming);

        if (streaming) {
            return invokeOpenAiStreaming(provider, openAiBody, account, inputTokens, outputTokens,
                    traceId, username, model, true);
        }
        return invokeOpenAiNonStreamingAsAnthropicSse(provider, openAiBody, account, model,
                inputTokens, outputTokens, traceId, username);
    }

    private Flux<ServerSentEvent<String>> invokeAnthropicStreaming(OtherProviderItem provider, String body,
                                                                    UpstreamAccountSelector.AccountSelection account,
                                                                    AtomicInteger inputTokens,
                                                                    AtomicInteger outputTokens,
                                                                    String traceId, String username, String userModel) {
        AtomicInteger eventCount = new AtomicInteger(0);
        return webClient.post()
                .uri(provider.resolvedBaseUrl() + "/v1/messages")
                .header("x-api-key", account.apiKey())
                .header("anthropic-version", ANTHROPIC_VERSION)
                .accept(MediaType.TEXT_EVENT_STREAM)
                .bodyValue(body)
                .exchangeToFlux(response -> readAnthropicSseOrError(provider, response))
                .map(this::normalizeAnthropicSseEvent)
                .doOnNext(event -> {
                    eventCount.incrementAndGet();
                    accumulateUsageFromAnthropicSse(event.data(), inputTokens, outputTokens);
                })
                .switchIfEmpty(Flux.defer(() -> Flux.error(new ProviderException(
                        "Provider '" + provider.name() + "' returned empty SSE stream"))))
                .doOnComplete(() -> log.info(
                        "HTTP passthrough Anthropic stream completed: provider={} traceId={} user={} model={} events={}",
                        provider.name(), traceId, username, userModel, eventCount.get()))
                .doOnError(e -> log.error("HTTP passthrough Anthropic stream error: provider={} traceId={} user={} model={}",
                        provider.name(), traceId, username, userModel, e));
    }

    private Flux<ServerSentEvent<String>> invokeAnthropicNonStreaming(OtherProviderItem provider, String body,
                                                                       UpstreamAccountSelector.AccountSelection account,
                                                                       String userModel,
                                                                       AtomicInteger inputTokens,
                                                                       AtomicInteger outputTokens,
                                                                       String traceId, String username) {
        return webClient.post()
                .uri(provider.resolvedBaseUrl() + "/v1/messages")
                .header("x-api-key", account.apiKey())
                .header("anthropic-version", ANTHROPIC_VERSION)
                .accept(MediaType.APPLICATION_JSON)
                .bodyValue(body)
                .retrieve()
                .onStatus(status -> status.isError(), response -> toProviderError(provider, response))
                .bodyToMono(String.class)
                .flatMapMany(json -> {
                    try {
                        if (json == null || json.isBlank()) {
                            return Flux.error(new ProviderException(
                                    "Provider '" + provider.name() + "' returned empty response body"));
                        }
                        // 部分供应商会用 HTTP 200 + error JSON 表示失败；不能当成功 SSE 回给 Claude Code
                        if (looksLikeUpstreamErrorJson(json)) {
                            return Flux.error(new ProviderException(
                                    "Provider '" + provider.name() + "' returned error payload: " + truncateForLog(json)));
                        }
                        accumulateUsageFromAnthropicMessage(json, inputTokens, outputTokens);
                        List<ServerSentEvent<String>> events = wrapAnthropicMessageAsSse(json, userModel);
                        log.info("HTTP passthrough Anthropic non-stream completed: provider={} traceId={} user={} model={} events={}",
                                provider.name(), traceId, username, userModel, events.size());
                        return Flux.fromIterable(events);
                    } catch (JsonProcessingException e) {
                        return Flux.error(new ProviderException(
                                "Failed to parse Anthropic response from provider '" + provider.name()
                                        + "': " + e.getMessage()));
                    }
                });
    }

    /**
     * @param convertToAnthropic 为 true 时将 OpenAI SSE chunk 转为 Anthropic SSE（Anthropic 客户端走 OpenAI 上游）
     */
    private Flux<ServerSentEvent<String>> invokeOpenAiStreaming(OtherProviderItem provider, String body,
                                                                 UpstreamAccountSelector.AccountSelection account,
                                                                 AtomicInteger inputTokens,
                                                                 AtomicInteger outputTokens,
                                                                 String traceId, String username, String userModel,
                                                                 boolean convertToAnthropic) {
        AtomicReference<AnthropicSseConverter> converterRef = new AtomicReference<>();
        AtomicInteger eventCount = new AtomicInteger(0);

        return webClient.post()
                .uri(provider.resolvedBaseUrl() + "/chat/completions")
                .header(HttpHeaders.AUTHORIZATION, "Bearer " + account.apiKey())
                .accept(MediaType.TEXT_EVENT_STREAM)
                .bodyValue(body)
                .exchangeToFlux(response -> readOpenAiSseOrError(provider, response))
                .flatMap(event -> {
                    String data = event.data();
                    if (data == null || data.isBlank()) {
                        return Flux.empty();
                    }
                    if ("[DONE]".equals(data.trim())) {
                        if (!convertToAnthropic) {
                            eventCount.incrementAndGet();
                            return Flux.just(ServerSentEvent.<String>builder().data("[DONE]").build());
                        }
                        return Flux.empty();
                    }
                    accumulateUsageFromOpenAiSse(data, inputTokens, outputTokens);
                    if (!convertToAnthropic) {
                        eventCount.incrementAndGet();
                        return Flux.just(ServerSentEvent.<String>builder().data(data).build());
                    }
                    AnthropicSseConverter converter = converterRef.updateAndGet(
                            c -> c != null ? c : new AnthropicSseConverter(objectMapper, userModel, traceId, false, 4096));
                    List<ServerSentEvent<String>> converted = converter.convert(data);
                    eventCount.addAndGet(converted.size());
                    return Flux.fromIterable(converted);
                })
                .concatWith(Flux.defer(() -> {
                    if (!convertToAnthropic) {
                        return Flux.empty();
                    }
                    AnthropicSseConverter converter = converterRef.get();
                    if (converter == null) {
                        return Flux.empty();
                    }
                    List<ServerSentEvent<String>> tail = converter.endStreamIfOpen();
                    eventCount.addAndGet(tail.size());
                    return Flux.fromIterable(tail);
                }))
                .switchIfEmpty(Flux.defer(() -> Flux.error(new ProviderException(
                        "Provider '" + provider.name() + "' returned empty OpenAI SSE stream"))))
                .doOnComplete(() -> log.info(
                        "HTTP passthrough OpenAI stream completed: provider={} traceId={} user={} model={} anthropicOut={} events={}",
                        provider.name(), traceId, username, userModel, convertToAnthropic, eventCount.get()))
                .doOnError(e -> log.error("HTTP passthrough OpenAI stream error: provider={} traceId={} user={} model={}",
                        provider.name(), traceId, username, userModel, e));
    }

    private Flux<ServerSentEvent<String>> invokeOpenAiNonStreamingAsAnthropicSse(OtherProviderItem provider, String body,
                                                                                  UpstreamAccountSelector.AccountSelection account,
                                                                                  String userModel,
                                                                                  AtomicInteger inputTokens,
                                                                                  AtomicInteger outputTokens,
                                                                                  String traceId, String username) {
        return webClient.post()
                .uri(provider.resolvedBaseUrl() + "/chat/completions")
                .header(HttpHeaders.AUTHORIZATION, "Bearer " + account.apiKey())
                .accept(MediaType.APPLICATION_JSON)
                .bodyValue(body)
                .retrieve()
                .onStatus(status -> status.isError(), response -> toProviderError(provider, response))
                .bodyToMono(String.class)
                .flatMapMany(json -> {
                    try {
                        accumulateUsageFromOpenAiMessage(json, inputTokens, outputTokens);
                        String anthropicJson = AnthropicOpenAiRequestConverter.toAnthropicMessage(
                                objectMapper, json, userModel);
                        List<ServerSentEvent<String>> events = wrapAnthropicMessageAsSse(anthropicJson, userModel);
                        log.info("HTTP passthrough OpenAI non-stream→Anthropic SSE: provider={} traceId={} user={} model={} events={}",
                                provider.name(), traceId, username, userModel, events.size());
                        return Flux.fromIterable(events);
                    } catch (JsonProcessingException e) {
                        return Flux.error(new ProviderException(
                                "Failed to parse OpenAI response from provider '" + provider.name()
                                        + "': " + e.getMessage()));
                    }
                });
    }

    private Flux<ServerSentEvent<String>> invokeOpenAiNonStreamingAsSingleEvent(OtherProviderItem provider, String body,
                                                                                 UpstreamAccountSelector.AccountSelection account,
                                                                                 AtomicInteger inputTokens,
                                                                                 AtomicInteger outputTokens,
                                                                                 String traceId, String username,
                                                                                 String userModel) {
        return webClient.post()
                .uri(provider.resolvedBaseUrl() + "/chat/completions")
                .header(HttpHeaders.AUTHORIZATION, "Bearer " + account.apiKey())
                .accept(MediaType.APPLICATION_JSON)
                .bodyValue(body)
                .retrieve()
                .onStatus(status -> status.isError(), response -> toProviderError(provider, response))
                .bodyToMono(String.class)
                .flatMapMany(json -> {
                    accumulateUsageFromOpenAiMessage(json, inputTokens, outputTokens);
                    log.info("HTTP passthrough OpenAI non-stream completed: provider={} traceId={} user={} model={}",
                            provider.name(), traceId, username, userModel);
                    return Flux.just(ServerSentEvent.<String>builder().data(json).build());
                });
    }

    /**
     * 替换上游模型 ID 并按视觉能力决定是否剥离图片块：
     * 仅当模型不支持视觉时才把 image/image_url 块替换为占位文本，
     * 视觉模型的图片必须原样保留（入口层未剥离时此处不能二次剥离）。
     */
    String prepareAnthropicRequestBody(String requestBody, String upstreamModelId, boolean streaming,
                                       boolean supportsVision)
            throws JsonProcessingException {
        String source = supportsVision ? requestBody : AnthropicMessageImageStripper.stripImageBlocks(
                objectMapper, requestBody, AnthropicMessageImageStripper.DEFAULT_PLACEHOLDER);
        JsonNode rootNode = objectMapper.readTree(source);
        if (!(rootNode instanceof ObjectNode root)) {
            throw new JsonProcessingException("Request body must be a JSON object") {};
        }
        root.put("model", upstreamModelId);
        root.put("stream", streaming);
        return objectMapper.writeValueAsString(root);
    }

    private String prepareOpenAiRequestBody(String openAiBody, String upstreamModelId, boolean streaming)
            throws JsonProcessingException {
        JsonNode rootNode = objectMapper.readTree(openAiBody);
        if (!(rootNode instanceof ObjectNode root)) {
            throw new JsonProcessingException("Request body must be a JSON object") {};
        }
        root.put("model", upstreamModelId);
        root.put("stream", streaming);
        return objectMapper.writeValueAsString(root);
    }

    private Mono<Throwable> toProviderError(OtherProviderItem provider,
                                            org.springframework.web.reactive.function.client.ClientResponse response) {
        return response.bodyToMono(String.class)
                .defaultIfEmpty("upstream error")
                .flatMap(msg -> Mono.error(new ProviderException(
                        "Provider '" + provider.name() + "' error " + response.statusCode().value() + ": " + msg)));
    }

    /**
     * 读取 Anthropic SSE；若上游以 HTTP 200 返回 JSON（常见限流/错误包装）则转为明确错误，避免 Claude Code 看到空 SSE。
     */
    private Flux<ServerSentEvent<String>> readAnthropicSseOrError(
            OtherProviderItem provider,
            org.springframework.web.reactive.function.client.ClientResponse response) {
        if (response.statusCode().isError()) {
            return toProviderError(provider, response).flatMapMany(Flux::error);
        }
        MediaType contentType = response.headers().contentType().orElse(MediaType.TEXT_EVENT_STREAM);
        if (isJsonContentType(contentType)) {
            return response.bodyToMono(String.class)
                    .defaultIfEmpty("")
                    .flatMapMany(json -> Flux.error(new ProviderException(
                            "Provider '" + provider.name()
                                    + "' returned JSON instead of SSE (HTTP "
                                    + response.statusCode().value() + "): " + truncateForLog(json))));
        }
        return response.bodyToFlux(new ParameterizedTypeReference<ServerSentEvent<String>>() {});
    }

    /**
     * 读取 OpenAI SSE；同样拦截 HTTP 200 + JSON 误用场景。
     */
    private Flux<ServerSentEvent<String>> readOpenAiSseOrError(
            OtherProviderItem provider,
            org.springframework.web.reactive.function.client.ClientResponse response) {
        if (response.statusCode().isError()) {
            return toProviderError(provider, response).flatMapMany(Flux::error);
        }
        MediaType contentType = response.headers().contentType().orElse(MediaType.TEXT_EVENT_STREAM);
        if (isJsonContentType(contentType)) {
            return response.bodyToMono(String.class)
                    .defaultIfEmpty("")
                    .flatMapMany(json -> Flux.error(new ProviderException(
                            "Provider '" + provider.name()
                                    + "' returned JSON instead of SSE (HTTP "
                                    + response.statusCode().value() + "): " + truncateForLog(json))));
        }
        return response.bodyToFlux(new ParameterizedTypeReference<ServerSentEvent<String>>() {});
    }

    /**
     * Anthropic 官方 SSE 要求 event: 行；部分供应商只在 data JSON 的 type 里带事件名。
     * 转发前补齐 event，避免 Claude Code 认为流畸形。
     */
    private ServerSentEvent<String> normalizeAnthropicSseEvent(ServerSentEvent<String> event) {
        if (event == null) {
            return ServerSentEvent.<String>builder().data("").build();
        }
        if (event.event() != null && !event.event().isBlank()) {
            return event;
        }
        String data = event.data();
        if (data == null || data.isBlank()) {
            return event;
        }
        try {
            JsonNode root = objectMapper.readTree(data);
            String type = root.path("type").asText(null);
            if (type != null && !type.isBlank()) {
                return ServerSentEvent.<String>builder()
                        .id(event.id())
                        .event(type)
                        .data(data)
                        .comment(event.comment())
                        .retry(event.retry())
                        .build();
            }
        } catch (Exception ignored) {
            // 非 JSON data 原样透传
        }
        return event;
    }

    private static boolean isJsonContentType(MediaType contentType) {
        return contentType != null && (
                MediaType.APPLICATION_JSON.isCompatibleWith(contentType)
                        || "json".equalsIgnoreCase(contentType.getSubtype())
                        || contentType.getSubtype().toLowerCase(Locale.ROOT).endsWith("+json"));
    }

    private boolean looksLikeUpstreamErrorJson(String json) {
        try {
            JsonNode root = objectMapper.readTree(json);
            if (root.has("error")) {
                return true;
            }
            String type = root.path("type").asText("");
            return "error".equals(type);
        } catch (Exception e) {
            return false;
        }
    }

    private static String truncateForLog(String text) {
        if (text == null) {
            return "";
        }
        String trimmed = text.trim();
        if (trimmed.length() <= 500) {
            return trimmed;
        }
        return trimmed.substring(0, 500) + "...";
    }

    private static String normalizeTraceId(String traceId) {
        return (traceId == null || traceId.isBlank()) ? "-" : traceId;
    }

    /** 请求体声明 stream 优先；否则看 capabilities 是否含 stream/streaming */
    private boolean resolveStreaming(ProviderConfig mapping, String requestBody) {
        try {
            JsonNode root = objectMapper.readTree(requestBody);
            if (root.has("stream")) {
                return root.get("stream").asBoolean(false);
            }
        } catch (Exception ignored) {
            // fall through
        }
        return supportsStreaming(mapping);
    }

    private boolean isStreamRequested(String body) {
        try {
            JsonNode root = objectMapper.readTree(body);
            if (root.has("stream")) {
                return root.get("stream").asBoolean(false);
            }
        } catch (Exception ignored) {
            // 默认非流式
        }
        return false;
    }

    /** 将 Anthropic 非流式 Message JSON 包装为 SSE 事件序列。 */
    private List<ServerSentEvent<String>> wrapAnthropicMessageAsSse(String json, String userModel)
            throws JsonProcessingException {
        JsonNode root = objectMapper.readTree(json);
        List<ServerSentEvent<String>> events = new ArrayList<>();

        String messageId = root.path("id").asText("msg_" + UUID.randomUUID().toString().replace("-", ""));
        int inputTok = root.path("usage").path("input_tokens").asInt(0);
        int outputTok = root.path("usage").path("output_tokens").asInt(0);

        ObjectNode messageStart = objectMapper.createObjectNode();
        messageStart.put("type", "message_start");
        ObjectNode message = messageStart.putObject("message");
        message.put("id", messageId);
        message.put("type", "message");
        message.put("role", "assistant");
        message.put("model", userModel);
        message.putArray("content");
        ObjectNode usageStart = message.putObject("usage");
        usageStart.put("input_tokens", inputTok);
        usageStart.put("output_tokens", 0);
        events.add(ServerSentEvent.builder(messageStart.toString()).event("message_start").build());

        JsonNode content = root.path("content");
        int blockIndex = 0;
        if (content.isArray()) {
            for (JsonNode block : content) {
                if ("text".equals(block.path("type").asText("text"))) {
                    ObjectNode blockStart = objectMapper.createObjectNode();
                    blockStart.put("type", "content_block_start");
                    blockStart.put("index", blockIndex);
                    ObjectNode startBlock = blockStart.putObject("content_block");
                    startBlock.put("type", "text");
                    startBlock.put("text", "");
                    events.add(ServerSentEvent.builder(blockStart.toString()).event("content_block_start").build());

                    String text = block.path("text").asText("");
                    if (!text.isEmpty()) {
                        ObjectNode delta = objectMapper.createObjectNode();
                        delta.put("type", "content_block_delta");
                        delta.put("index", blockIndex);
                        ObjectNode deltaBlock = delta.putObject("delta");
                        deltaBlock.put("type", "text_delta");
                        deltaBlock.put("text", text);
                        events.add(ServerSentEvent.builder(delta.toString()).event("content_block_delta").build());
                    }

                    ObjectNode blockStop = objectMapper.createObjectNode();
                    blockStop.put("type", "content_block_stop");
                    blockStop.put("index", blockIndex);
                    events.add(ServerSentEvent.builder(blockStop.toString()).event("content_block_stop").build());
                    blockIndex++;
                }
            }
        }

        ObjectNode messageDelta = objectMapper.createObjectNode();
        messageDelta.put("type", "message_delta");
        ObjectNode deltaUsage = messageDelta.putObject("usage");
        deltaUsage.put("output_tokens", outputTok);
        messageDelta.put("stop_reason", root.path("stop_reason").asText("end_turn"));
        messageDelta.put("stop_sequence", (String) null);
        events.add(ServerSentEvent.builder(messageDelta.toString()).event("message_delta").build());

        ObjectNode messageStop = objectMapper.createObjectNode();
        messageStop.put("type", "message_stop");
        events.add(ServerSentEvent.builder(messageStop.toString()).event("message_stop").build());
        return events;
    }

    private void accumulateUsageFromAnthropicSse(String data, AtomicInteger inputTokens, AtomicInteger outputTokens) {
        if (data == null || data.isBlank()) {
            return;
        }
        try {
            JsonNode root = objectMapper.readTree(data);
            String type = root.path("type").asText("");
            if ("message_start".equals(type)) {
                int input = root.at("/message/usage/input_tokens").asInt(-1);
                if (input >= 0) {
                    inputTokens.set(input);
                }
            } else if ("message_delta".equals(type)) {
                int output = root.at("/usage/output_tokens").asInt(-1);
                if (output >= 0) {
                    outputTokens.set(output);
                }
            }
        } catch (Exception ignored) {
            // 解析失败时忽略
        }
    }

    private void accumulateUsageFromAnthropicMessage(String json, AtomicInteger inputTokens, AtomicInteger outputTokens)
            throws JsonProcessingException {
        JsonNode usage = objectMapper.readTree(json).path("usage");
        if (!usage.isMissingNode()) {
            inputTokens.set(usage.path("input_tokens").asInt(0));
            outputTokens.set(usage.path("output_tokens").asInt(0));
        }
    }

    private void accumulateUsageFromOpenAiSse(String data, AtomicInteger inputTokens, AtomicInteger outputTokens) {
        try {
            JsonNode root = objectMapper.readTree(data);
            JsonNode usage = root.path("usage");
            if (!usage.isMissingNode()) {
                int prompt = usage.path("prompt_tokens").asInt(-1);
                int completion = usage.path("completion_tokens").asInt(-1);
                if (prompt >= 0) {
                    inputTokens.set(prompt);
                }
                if (completion >= 0) {
                    outputTokens.set(completion);
                }
            }
        } catch (Exception ignored) {
            // 忽略
        }
    }

    private void accumulateUsageFromOpenAiMessage(String json, AtomicInteger inputTokens, AtomicInteger outputTokens) {
        try {
            JsonNode usage = objectMapper.readTree(json).path("usage");
            if (!usage.isMissingNode()) {
                inputTokens.set(usage.path("prompt_tokens").asInt(0));
                outputTokens.set(usage.path("completion_tokens").asInt(0));
            }
        } catch (Exception ignored) {
            // 忽略
        }
    }

    private boolean supportsStreaming(ProviderConfig mapping) {
        return mapping.capabilities().stream()
                .filter(c -> c != null)
                .map(c -> c.toLowerCase(Locale.ROOT))
                .anyMatch(c -> c.equals("stream") || c.equals("streaming"));
    }
}
