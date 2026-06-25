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

/**
 * 华为云 MaaS Anthropic 兼容接口代理：将网关 Anthropic 请求转发至 {@code huawei-maas.base-url/v1/messages}。
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
                .baseUrl(normalizeBaseUrl(props.baseUrl()))
                .defaultHeader(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
                .build();
    }

    /**
     * 转发请求到华为云 MaaS 并返回 Anthropic 格式 SSE 流。
     */
    public Flux<ServerSentEvent<String>> forward(ProviderConfig mapping, String requestBody,
                                                  AtomicInteger inputTokens, AtomicInteger outputTokens,
                                                  String username, String model, String traceId) {
        if (props.apiKey() == null || props.apiKey().isBlank()) {
            return Flux.error(new ProviderException("Huawei MaaS API key is not configured"));
        }

        final String correlationId = (traceId == null || traceId.isBlank()) ? "-" : traceId;
        boolean streaming = supportsStreaming(mapping);
        String body;
        try {
            body = prepareRequestBody(requestBody, mapping.upstreamModelId(), streaming);
        } catch (JsonProcessingException e) {
            return Flux.error(new ProviderException("Invalid request body for Huawei MaaS: " + e.getMessage()));
        }

        log.info("Huawei MaaS request outbound: traceId={} user={} userModel={} upstreamModel={} streaming={}",
                correlationId, username, model, mapping.upstreamModelId(), streaming);

        if (streaming) {
            return invokeStreaming(body, model, inputTokens, outputTokens, correlationId, username);
        }
        return invokeNonStreaming(body, model, inputTokens, outputTokens, correlationId, username);
    }

    private Flux<ServerSentEvent<String>> invokeStreaming(String body, String userModel,
                                                            AtomicInteger inputTokens, AtomicInteger outputTokens,
                                                            String traceId, String username) {
        return webClient.post()
                .uri("/v1/messages")
                .header("x-api-key", props.apiKey())
                .header("anthropic-version", ANTHROPIC_VERSION)
                .accept(MediaType.TEXT_EVENT_STREAM)
                .bodyValue(body)
                .retrieve()
                .onStatus(status -> status.isError(), response -> response.bodyToMono(String.class)
                        .defaultIfEmpty("Huawei MaaS upstream error")
                        .flatMap(msg -> Mono.error(new ProviderException(
                                "Huawei MaaS error " + response.statusCode().value() + ": " + msg))))
                .bodyToFlux(new ParameterizedTypeReference<ServerSentEvent<String>>() {})
                .doOnNext(event -> accumulateUsageFromSse(event.data(), inputTokens, outputTokens))
                .doOnComplete(() -> log.info("Huawei MaaS stream completed: traceId={} user={} model={}",
                        traceId, username, userModel))
                .doOnError(e -> log.error("Huawei MaaS stream error: traceId={} user={} model={}",
                        traceId, username, userModel, e));
    }

    private Flux<ServerSentEvent<String>> invokeNonStreaming(String body, String userModel,
                                                                AtomicInteger inputTokens, AtomicInteger outputTokens,
                                                                String traceId, String username) {
        return webClient.post()
                .uri("/v1/messages")
                .header("x-api-key", props.apiKey())
                .header("anthropic-version", ANTHROPIC_VERSION)
                .accept(MediaType.APPLICATION_JSON)
                .bodyValue(body)
                .retrieve()
                .onStatus(status -> status.isError(), response -> response.bodyToMono(String.class)
                        .defaultIfEmpty("Huawei MaaS upstream error")
                        .flatMap(msg -> Mono.error(new ProviderException(
                                "Huawei MaaS error " + response.statusCode().value() + ": " + msg))))
                .bodyToMono(String.class)
                .flatMapMany(json -> {
                    try {
                        accumulateUsageFromMessage(json, inputTokens, outputTokens);
                        List<ServerSentEvent<String>> events = wrapAnthropicMessageAsSse(json, userModel);
                        log.info("Huawei MaaS non-stream completed: traceId={} user={} model={} events={}",
                                traceId, username, userModel, events.size());
                        return Flux.fromIterable(events);
                    } catch (JsonProcessingException e) {
                        return Flux.error(new ProviderException("Failed to parse Huawei MaaS response: " + e.getMessage()));
                    }
                });
    }

    /**
     * 将请求体中的 model 替换为上游模型 ID，并按能力声明设置 stream 字段。
     * 华为 MaaS 当前仅支持 text content，需剥离历史会话中残留的 image / image_url 块。
     */
    private String prepareRequestBody(String requestBody, String upstreamModelId, boolean streaming)
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

    /** 将 Anthropic 非流式 Message JSON 包装为 SSE 事件序列，供网关统一以 SSE 返回。 */
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
                String blockType = block.path("type").asText("text");
                if ("text".equals(blockType)) {
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

    private void accumulateUsageFromSse(String data, AtomicInteger inputTokens, AtomicInteger outputTokens) {
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
        } catch (Exception e) {
            // 解析失败时忽略，不影响流式转发
        }
    }

    private void accumulateUsageFromMessage(String json, AtomicInteger inputTokens, AtomicInteger outputTokens)
            throws JsonProcessingException {
        JsonNode root = objectMapper.readTree(json);
        JsonNode usage = root.path("usage");
        if (!usage.isMissingNode()) {
            inputTokens.set(usage.path("input_tokens").asInt(0));
            outputTokens.set(usage.path("output_tokens").asInt(0));
        }
    }

    private boolean supportsStreaming(ProviderConfig mapping) {
        return mapping.capabilities().stream()
                .filter(c -> c != null)
                .map(c -> c.toLowerCase(Locale.ROOT))
                .anyMatch(c -> c.equals("stream") || c.equals("streaming"));
    }

    private static String normalizeBaseUrl(String baseUrl) {
        String trimmed = baseUrl.trim();
        while (trimmed.endsWith("/")) {
            trimmed = trimmed.substring(0, trimmed.length() - 1);
        }
        return trimmed;
    }
}
