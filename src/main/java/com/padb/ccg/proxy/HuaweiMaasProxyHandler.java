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
 * 华为云 MaaS 代理：支持 Anthropic（{@code /v1/messages}）与 OpenAI（{@code /chat/completions}）两种上游协议。
 * 对网关 {@link com.padb.ccg.proxy.ProxyService} 统一输出 Anthropic SSE；对 OpenAI 客户端可透传 OpenAI SSE。
 */
@Component
public class HuaweiMaasProxyHandler {

    private static final Logger log = LoggerFactory.getLogger(HuaweiMaasProxyHandler.class);
    private static final String ANTHROPIC_VERSION = "2023-06-01";

    private final HuaweiMaasProperties props;
    private final ObjectMapper objectMapper;
    private final WebClient webClient;

    public HuaweiMaasProxyHandler(HuaweiMaasProperties props, ObjectMapper objectMapper, WebClient.Builder builder) {
        this.props = props;
        this.objectMapper = objectMapper;
        Duration timeout = Duration.ofSeconds(Math.max(1, props.timeoutSeconds()));
        HttpClient httpClient = HttpClient.create().responseTimeout(timeout);
        this.webClient = builder
                .clientConnector(new ReactorClientHttpConnector(httpClient))
                .baseUrl(props.resolvedBaseUrl())
                .defaultHeader(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
                .build();
    }

    /**
     * 转发 Anthropic 格式请求，返回 Anthropic SSE（供 {@code /v1/messages} 及 Bedrock 风格链路使用）。
     */
    public Flux<ServerSentEvent<String>> forward(ProviderConfig mapping, String requestBody,
                                                  AtomicInteger inputTokens, AtomicInteger outputTokens,
                                                  String username, String model, String traceId) {
        UpstreamAccountSelector.HuaweiSelection account = UpstreamAccountSelector.selectHuawei(mapping, props);
        final String correlationId = normalizeTraceId(traceId);
        boolean streaming = supportsStreaming(mapping);

        if (props.isOpenAiFormat()) {
            return forwardAnthropicViaOpenAiUpstream(mapping, requestBody, inputTokens, outputTokens,
                    username, model, correlationId, streaming, account);
        }
        return forwardAnthropicUpstream(mapping, requestBody, inputTokens, outputTokens,
                username, model, correlationId, streaming, account);
    }

    /**
     * 透传 OpenAI Chat Completions 请求/响应（仅 {@code api-format=openai} 时可用）。
     */
    public Flux<ServerSentEvent<String>> forwardOpenAi(ProviderConfig mapping, String openAiBody,
                                                        AtomicInteger inputTokens, AtomicInteger outputTokens,
                                                        String username, String model, String traceId) {
        if (!props.isOpenAiFormat()) {
            return Flux.error(new ProviderException(
                    "Huawei MaaS OpenAI passthrough requires huawei-maas.api-format=openai"));
        }

        UpstreamAccountSelector.HuaweiSelection account = UpstreamAccountSelector.selectHuawei(mapping, props);
        final String correlationId = normalizeTraceId(traceId);
        boolean streaming = isStreamRequested(openAiBody);
        String body;
        try {
            body = prepareOpenAiRequestBody(openAiBody, mapping.upstreamModelId(), streaming);
        } catch (JsonProcessingException e) {
            return Flux.error(new ProviderException("Invalid OpenAI request for Huawei MaaS: " + e.getMessage()));
        }

        log.info("Huawei MaaS OpenAI passthrough: traceId={} user={} userModel={} upstreamModel={} account={} streaming={}",
                correlationId, username, model, mapping.upstreamModelId(), account.accountId(), streaming);

        if (streaming) {
            return invokeOpenAiStreaming(body, account.apiKey(), inputTokens, outputTokens, correlationId, username, model, false);
        }
        return invokeOpenAiNonStreamingAsSingleEvent(body, account.apiKey(), inputTokens, outputTokens, correlationId, username, model);
    }

    private Flux<ServerSentEvent<String>> forwardAnthropicUpstream(ProviderConfig mapping, String requestBody,
                                                                   AtomicInteger inputTokens, AtomicInteger outputTokens,
                                                                   String username, String model, String traceId,
                                                                   boolean streaming,
                                                                   UpstreamAccountSelector.HuaweiSelection account) {
        String body;
        try {
            body = prepareAnthropicRequestBody(requestBody, mapping.upstreamModelId(), streaming);
        } catch (JsonProcessingException e) {
            return Flux.error(new ProviderException("Invalid request body for Huawei MaaS: " + e.getMessage()));
        }

        log.info("Huawei MaaS Anthropic upstream: traceId={} user={} userModel={} upstreamModel={} account={} streaming={}",
                traceId, username, model, mapping.upstreamModelId(), account.accountId(), streaming);

        if (streaming) {
            return invokeAnthropicStreaming(body, account.apiKey(), inputTokens, outputTokens, traceId, username, model);
        }
        return invokeAnthropicNonStreaming(body, account.apiKey(), model, inputTokens, outputTokens, traceId, username);
    }

    private Flux<ServerSentEvent<String>> forwardAnthropicViaOpenAiUpstream(ProviderConfig mapping, String requestBody,
                                                                             AtomicInteger inputTokens,
                                                                             AtomicInteger outputTokens,
                                                                             String username, String model,
                                                                             String traceId, boolean streaming,
                                                                             UpstreamAccountSelector.HuaweiSelection account) {
        String openAiBody;
        try {
            openAiBody = AnthropicOpenAiRequestConverter.toOpenAiChat(
                    objectMapper, requestBody, mapping.upstreamModelId(), streaming);
        } catch (JsonProcessingException e) {
            return Flux.error(new ProviderException("Failed to convert Anthropic request for Huawei OpenAI API: "
                    + e.getMessage()));
        }

        log.info("Huawei MaaS OpenAI upstream (Anthropic client): traceId={} user={} userModel={} upstreamModel={} account={} streaming={}",
                traceId, username, model, mapping.upstreamModelId(), account.accountId(), streaming);

        if (streaming) {
            return invokeOpenAiStreaming(openAiBody, account.apiKey(), inputTokens, outputTokens, traceId, username, model, true);
        }
        return invokeOpenAiNonStreamingAsAnthropicSse(openAiBody, account.apiKey(), model, inputTokens, outputTokens, traceId, username);
    }

    private Flux<ServerSentEvent<String>> invokeAnthropicStreaming(String body, String apiKey,
                                                                    AtomicInteger inputTokens,
                                                                    AtomicInteger outputTokens,
                                                                    String traceId, String username, String userModel) {
        return webClient.post()
                .uri("/v1/messages")
                .header("x-api-key", apiKey)
                .header("anthropic-version", ANTHROPIC_VERSION)
                .accept(MediaType.TEXT_EVENT_STREAM)
                .bodyValue(body)
                .retrieve()
                .onStatus(status -> status.isError(), this::toProviderError)
                .bodyToFlux(new ParameterizedTypeReference<ServerSentEvent<String>>() {})
                .doOnNext(event -> accumulateUsageFromAnthropicSse(event.data(), inputTokens, outputTokens))
                .doOnComplete(() -> log.info("Huawei MaaS Anthropic stream completed: traceId={} user={} model={}",
                        traceId, username, userModel))
                .doOnError(e -> log.error("Huawei MaaS Anthropic stream error: traceId={} user={} model={}",
                        traceId, username, userModel, e));
    }

    private Flux<ServerSentEvent<String>> invokeAnthropicNonStreaming(String body, String apiKey, String userModel,
                                                                       AtomicInteger inputTokens,
                                                                       AtomicInteger outputTokens,
                                                                       String traceId, String username) {
        return webClient.post()
                .uri("/v1/messages")
                .header("x-api-key", apiKey)
                .header("anthropic-version", ANTHROPIC_VERSION)
                .accept(MediaType.APPLICATION_JSON)
                .bodyValue(body)
                .retrieve()
                .onStatus(status -> status.isError(), this::toProviderError)
                .bodyToMono(String.class)
                .flatMapMany(json -> {
                    try {
                        accumulateUsageFromAnthropicMessage(json, inputTokens, outputTokens);
                        List<ServerSentEvent<String>> events = wrapAnthropicMessageAsSse(json, userModel);
                        log.info("Huawei MaaS Anthropic non-stream completed: traceId={} user={} model={} events={}",
                                traceId, username, userModel, events.size());
                        return Flux.fromIterable(events);
                    } catch (JsonProcessingException e) {
                        return Flux.error(new ProviderException(
                                "Failed to parse Huawei MaaS Anthropic response: " + e.getMessage()));
                    }
                });
    }

    /**
     * @param convertToAnthropic 为 true 时将 OpenAI SSE chunk 转为 Anthropic SSE（Anthropic 客户端走 OpenAI 上游）
     */
    private Flux<ServerSentEvent<String>> invokeOpenAiStreaming(String body, String apiKey,
                                                                 AtomicInteger inputTokens,
                                                                 AtomicInteger outputTokens,
                                                                 String traceId, String username, String userModel,
                                                                 boolean convertToAnthropic) {
        AtomicReference<AnthropicSseConverter> converterRef = new AtomicReference<>();

        return webClient.post()
                .uri("/chat/completions")
                .header(HttpHeaders.AUTHORIZATION, "Bearer " + apiKey)
                .accept(MediaType.TEXT_EVENT_STREAM)
                .bodyValue(body)
                .retrieve()
                .onStatus(status -> status.isError(), this::toProviderError)
                .bodyToFlux(new ParameterizedTypeReference<ServerSentEvent<String>>() {})
                .flatMap(event -> {
                    String data = event.data();
                    if (data == null || data.isBlank()) {
                        return Flux.empty();
                    }
                    if ("[DONE]".equals(data.trim())) {
                        if (!convertToAnthropic) {
                            return Flux.just(ServerSentEvent.<String>builder().data("[DONE]").build());
                        }
                        return Flux.empty();
                    }
                    accumulateUsageFromOpenAiSse(data, inputTokens, outputTokens);
                    if (!convertToAnthropic) {
                        return Flux.just(ServerSentEvent.<String>builder().data(data).build());
                    }
                    AnthropicSseConverter converter = converterRef.updateAndGet(
                            c -> c != null ? c : new AnthropicSseConverter(objectMapper, userModel, traceId, false, 4096));
                    return Flux.fromIterable(converter.convert(data));
                })
                .concatWith(Flux.defer(() -> {
                    if (!convertToAnthropic) {
                        return Flux.empty();
                    }
                    AnthropicSseConverter converter = converterRef.get();
                    if (converter == null) {
                        return Flux.empty();
                    }
                    return Flux.fromIterable(converter.endStreamIfOpen());
                }))
                .doOnComplete(() -> log.info("Huawei MaaS OpenAI stream completed: traceId={} user={} model={} anthropicOut={}",
                        traceId, username, userModel, convertToAnthropic))
                .doOnError(e -> log.error("Huawei MaaS OpenAI stream error: traceId={} user={} model={}",
                        traceId, username, userModel, e));
    }

    private Flux<ServerSentEvent<String>> invokeOpenAiNonStreamingAsAnthropicSse(String body, String apiKey,
                                                                                  String userModel,
                                                                                  AtomicInteger inputTokens,
                                                                                  AtomicInteger outputTokens,
                                                                                  String traceId, String username) {
        return webClient.post()
                .uri("/chat/completions")
                .header(HttpHeaders.AUTHORIZATION, "Bearer " + apiKey)
                .accept(MediaType.APPLICATION_JSON)
                .bodyValue(body)
                .retrieve()
                .onStatus(status -> status.isError(), this::toProviderError)
                .bodyToMono(String.class)
                .flatMapMany(json -> {
                    try {
                        accumulateUsageFromOpenAiMessage(json, inputTokens, outputTokens);
                        String anthropicJson = AnthropicOpenAiRequestConverter.toAnthropicMessage(
                                objectMapper, json, userModel);
                        List<ServerSentEvent<String>> events = wrapAnthropicMessageAsSse(anthropicJson, userModel);
                        log.info("Huawei MaaS OpenAI non-stream→Anthropic SSE: traceId={} user={} model={} events={}",
                                traceId, username, userModel, events.size());
                        return Flux.fromIterable(events);
                    } catch (JsonProcessingException e) {
                        return Flux.error(new ProviderException(
                                "Failed to parse Huawei MaaS OpenAI response: " + e.getMessage()));
                    }
                });
    }

    private Flux<ServerSentEvent<String>> invokeOpenAiNonStreamingAsSingleEvent(String body, String apiKey,
                                                                                 AtomicInteger inputTokens,
                                                                                 AtomicInteger outputTokens,
                                                                                 String traceId, String username,
                                                                                 String userModel) {
        return webClient.post()
                .uri("/chat/completions")
                .header(HttpHeaders.AUTHORIZATION, "Bearer " + apiKey)
                .accept(MediaType.APPLICATION_JSON)
                .bodyValue(body)
                .retrieve()
                .onStatus(status -> status.isError(), this::toProviderError)
                .bodyToMono(String.class)
                .flatMapMany(json -> {
                    accumulateUsageFromOpenAiMessage(json, inputTokens, outputTokens);
                    log.info("Huawei MaaS OpenAI non-stream completed: traceId={} user={} model={}",
                            traceId, username, userModel);
                    return Flux.just(ServerSentEvent.<String>builder().data(json).build());
                });
    }

    private String prepareAnthropicRequestBody(String requestBody, String upstreamModelId, boolean streaming)
            throws JsonProcessingException {
        String stripped = AnthropicMessageImageStripper.stripImageBlocks(
                objectMapper, requestBody, AnthropicMessageImageStripper.DEFAULT_PLACEHOLDER);
        JsonNode rootNode = objectMapper.readTree(stripped);
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

    private Mono<Throwable> toProviderError(org.springframework.web.reactive.function.client.ClientResponse response) {
        return response.bodyToMono(String.class)
                .defaultIfEmpty("Huawei MaaS upstream error")
                .flatMap(msg -> Mono.error(new ProviderException(
                        "Huawei MaaS error " + response.statusCode().value() + ": " + msg)));
    }

    private static String normalizeTraceId(String traceId) {
        return (traceId == null || traceId.isBlank()) ? "-" : traceId;
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
