package com.padb.ccg.proxy;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.codec.ServerSentEvent;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * 将 Bedrock/Anthropic SSE 事件流转换为 OpenAI Responses API 流式事件。
 *
 * <p>客户端（如 OpenCode {@code @ai-sdk/openai}）依赖完整生命周期事件：
 * {@code response.created} → {@code response.output_text.delta} → {@code response.completed} 等。
 */
public class OpenAiResponsesSseConverter {

    private static final Logger log = LoggerFactory.getLogger(OpenAiResponsesSseConverter.class);

    private final ObjectMapper objectMapper;
    private final String model;
    private final String responseId;
    private final String messageItemId;
    private final AtomicInteger sequenceNumber = new AtomicInteger(0);

    private final StringBuilder contentBuffer = new StringBuilder();
    private final AtomicInteger inputTokens = new AtomicInteger(0);
    private final AtomicInteger outputTokens = new AtomicInteger(0);
    private final AtomicReference<String> finishStatus = new AtomicReference<>("completed");

    private final AtomicBoolean streamStarted = new AtomicBoolean(false);
    private final AtomicBoolean messageItemOpened = new AtomicBoolean(false);
    private final AtomicBoolean textPartOpened = new AtomicBoolean(false);
    private final AtomicBoolean streamCompleted = new AtomicBoolean(false);

    private int outputIndex = 0;
    private int contentIndex = 0;
    private int activeToolOutputIndex = -1;
    private String activeToolItemId;
    private String activeToolName;
    private final StringBuilder toolArgumentsBuffer = new StringBuilder();

    public OpenAiResponsesSseConverter(ObjectMapper objectMapper, String model) {
        this.objectMapper = objectMapper;
        this.model = model;
        this.responseId = "resp_" + UUID.randomUUID().toString().replace("-", "").substring(0, 24);
        this.messageItemId = "msg_" + UUID.randomUUID().toString().replace("-", "").substring(0, 24);
    }

    /**
     * 流开始时发送 Responses API 前置生命周期事件。
     */
    public List<ServerSentEvent<String>> startStream() {
        List<ServerSentEvent<String>> results = new ArrayList<>();
        if (!streamStarted.compareAndSet(false, true)) {
            return results;
        }

        ObjectNode responseShell = buildResponseShell("in_progress");
        emitTypedEvent(results, "response.created", event -> {
            event.set("response", responseShell.deepCopy());
        });
        emitTypedEvent(results, "response.in_progress", event -> {
            event.set("response", responseShell.deepCopy());
        });
        return results;
    }

    /**
     * 将单条 Anthropic SSE data 转为 Responses API 事件列表。
     */
    public List<ServerSentEvent<String>> convert(String anthropicData) {
        List<ServerSentEvent<String>> results = new ArrayList<>();
        results.addAll(startStream());

        try {
            JsonNode root = objectMapper.readTree(anthropicData);
            String eventType = root.has("type") ? root.get("type").asText() : null;

            switch (eventType != null ? eventType : "") {
                case "message_start" -> handleMessageStart(root);
                case "content_block_start" -> handleContentBlockStart(root, results);
                case "content_block_delta" -> handleContentBlockDelta(root, results);
                case "message_delta" -> handleMessageDelta(root);
                case "message_stop" -> handleMessageStop(results);
                default -> handleBedrockRaw(anthropicData, results);
            }
        } catch (Exception e) {
            log.warn("Failed to parse Anthropic event for Responses API: {}", e.getMessage());
            handleBedrockRaw(anthropicData, results);
        }

        return results;
    }

    /**
     * 上游流正常结束但尚未发送 completed 时，补齐收尾事件。
     */
    public List<ServerSentEvent<String>> endStreamIfOpen() {
        List<ServerSentEvent<String>> results = new ArrayList<>();
        if (streamCompleted.get()) {
            return results;
        }
        if (messageItemOpened.get() && textPartOpened.get()) {
            emitTextDone(results);
            emitContentPartDone(results);
            emitMessageItemDone(results);
        }
        if (activeToolOutputIndex >= 0 && activeToolItemId != null) {
            emitFunctionCallDone(results);
            emitFunctionCallItemDone(results, activeToolOutputIndex, activeToolItemId, activeToolName,
                    toolArgumentsBuffer.toString());
        }
        emitCompleted(results);
        return results;
    }

    public int getInputTokens() {
        return inputTokens.get();
    }

    public int getOutputTokens() {
        return outputTokens.get();
    }

    public String getResponseId() {
        return responseId;
    }

    public String getAccumulatedText() {
        return contentBuffer.toString();
    }

    public String getFinishStatus() {
        return finishStatus.get();
    }

    private void handleMessageStart(JsonNode root) {
        JsonNode message = root.path("message");
        if (message.has("usage") && message.get("usage").has("input_tokens")) {
            inputTokens.set(message.get("usage").get("input_tokens").asInt());
        }
    }

    private void handleContentBlockStart(JsonNode root, List<ServerSentEvent<String>> results) {
        int index = root.path("index").asInt();
        JsonNode contentBlock = root.path("content_block");
        String type = contentBlock.path("type").asText();

        if ("tool_use".equals(type)) {
            activeToolOutputIndex = outputIndex++;
            activeToolItemId = contentBlock.path("id").asText("call_" + index);
            activeToolName = contentBlock.path("name").asText("");
            toolArgumentsBuffer.setLength(0);

            int toolOutputIndex = activeToolOutputIndex;
            String toolItemId = activeToolItemId;
            String toolName = activeToolName;
            emitTypedEvent(results, "response.output_item.added", event -> {
                event.put("output_index", toolOutputIndex);
                ObjectNode item = event.putObject("item");
                item.put("type", "function_call");
                item.put("id", toolItemId);
                item.put("call_id", toolItemId);
                item.put("name", toolName);
                item.put("status", "in_progress");
                item.putNull("arguments");
            });
            return;
        }

        ensureMessageItemStarted(results);
        if (!textPartOpened.get()) {
            textPartOpened.set(true);
            emitTypedEvent(results, "response.content_part.added", event -> {
                event.put("item_id", messageItemId);
                event.put("output_index", 0);
                event.put("content_index", contentIndex);
                ObjectNode part = event.putObject("part");
                part.put("type", "output_text");
                part.put("text", "");
                part.putArray("annotations");
            });
        }
    }

    private void handleContentBlockDelta(JsonNode root, List<ServerSentEvent<String>> results) {
        JsonNode delta = root.path("delta");
        String deltaType = delta.path("type").asText();

        if ("text_delta".equals(deltaType)) {
            ensureMessageItemStarted(results);
            if (!textPartOpened.get()) {
                handleContentBlockStart(objectMapper.createObjectNode()
                        .put("index", 0)
                        .set("content_block", objectMapper.createObjectNode().put("type", "text")), results);
            }
            String text = delta.path("text").asText("");
            if (!text.isEmpty()) {
                contentBuffer.append(text);
                String deltaText = text;
                emitTypedEvent(results, "response.output_text.delta", event -> {
                    event.put("item_id", messageItemId);
                    event.put("output_index", 0);
                    event.put("content_index", contentIndex);
                    event.put("delta", deltaText);
                });
            }
            return;
        }

        if ("input_json_delta".equals(deltaType) && activeToolItemId != null) {
            String partialJson = delta.path("partial_json").asText("");
            if (!partialJson.isEmpty()) {
                toolArgumentsBuffer.append(partialJson);
                String toolItemId = activeToolItemId;
                int toolOutputIndex = activeToolOutputIndex;
                String argsDelta = partialJson;
                emitTypedEvent(results, "response.function_call_arguments.delta", event -> {
                    event.put("item_id", toolItemId);
                    event.put("output_index", toolOutputIndex);
                    event.put("delta", argsDelta);
                });
            }
        }
    }

    private void handleMessageDelta(JsonNode root) {
        JsonNode delta = root.path("delta");
        JsonNode usage = root.path("usage");

        if (delta.has("stop_reason")) {
            String stopReason = delta.get("stop_reason").asText();
            if ("max_tokens".equals(stopReason)) {
                finishStatus.set("incomplete");
            } else if ("tool_use".equals(stopReason)) {
                finishStatus.set("completed");
            }
        }
        if (usage.has("output_tokens")) {
            outputTokens.set(usage.get("output_tokens").asInt());
        }
    }

    private void handleMessageStop(List<ServerSentEvent<String>> results) {
        if (activeToolItemId != null) {
            emitFunctionCallDone(results);
            emitFunctionCallItemDone(results, activeToolOutputIndex, activeToolItemId, activeToolName,
                    toolArgumentsBuffer.toString());
            activeToolItemId = null;
            activeToolOutputIndex = -1;
        } else if (messageItemOpened.get()) {
            if (textPartOpened.get()) {
                emitTextDone(results);
                emitContentPartDone(results);
            }
            emitMessageItemDone(results);
        }
        emitCompleted(results);
    }

    private void handleBedrockRaw(String data, List<ServerSentEvent<String>> results) {
        try {
            JsonNode root = objectMapper.readTree(data);
            if (root.has("content") && root.get("content").isArray()) {
                for (JsonNode block : root.get("content")) {
                    if ("text".equals(block.path("type").asText())) {
                        String text = block.path("text").asText("");
                        if (!text.isEmpty()) {
                            contentBuffer.append(text);
                            ensureMessageItemStarted(results);
                            if (!textPartOpened.get()) {
                                handleContentBlockStart(objectMapper.createObjectNode()
                                        .put("index", 0)
                                        .set("content_block", objectMapper.createObjectNode().put("type", "text")), results);
                            }
                            String deltaText = text;
                            emitTypedEvent(results, "response.output_text.delta", event -> {
                                event.put("item_id", messageItemId);
                                event.put("output_index", 0);
                                event.put("content_index", contentIndex);
                                event.put("delta", deltaText);
                            });
                        }
                    }
                }
            } else if (root.has("completion")) {
                String completion = root.get("completion").asText("");
                if (!completion.isEmpty()) {
                    contentBuffer.append(completion);
                    ensureMessageItemStarted(results);
                    String deltaText = completion;
                    emitTypedEvent(results, "response.output_text.delta", event -> {
                        event.put("item_id", messageItemId);
                        event.put("output_index", 0);
                        event.put("content_index", contentIndex);
                        event.put("delta", deltaText);
                    });
                }
            }

            if (root.has("usage")) {
                JsonNode usage = root.get("usage");
                if (usage.has("input_tokens")) {
                    inputTokens.set(usage.get("input_tokens").asInt());
                }
                if (usage.has("output_tokens")) {
                    outputTokens.set(usage.get("output_tokens").asInt());
                }
            }

            if (root.has("stop_reason")) {
                if (messageItemOpened.get()) {
                    emitTextDone(results);
                    emitContentPartDone(results);
                    emitMessageItemDone(results);
                }
                emitCompleted(results);
            }
        } catch (Exception e) {
            log.warn("Failed to parse Bedrock raw response for Responses API: {}", e.getMessage());
        }
    }

    private void ensureMessageItemStarted(List<ServerSentEvent<String>> results) {
        if (messageItemOpened.compareAndSet(false, true)) {
            emitTypedEvent(results, "response.output_item.added", event -> {
                event.put("output_index", 0);
                ObjectNode item = event.putObject("item");
                item.put("type", "message");
                item.put("id", messageItemId);
                item.put("role", "assistant");
                item.put("status", "in_progress");
                item.putArray("content");
            });
        }
    }

    private void emitTextDone(List<ServerSentEvent<String>> results) {
        String finalText = contentBuffer.toString();
        emitTypedEvent(results, "response.output_text.done", event -> {
            event.put("item_id", messageItemId);
            event.put("output_index", 0);
            event.put("content_index", contentIndex);
            event.put("text", finalText);
        });
    }

    private void emitContentPartDone(List<ServerSentEvent<String>> results) {
        emitTypedEvent(results, "response.content_part.done", event -> {
            event.put("item_id", messageItemId);
            event.put("output_index", 0);
            event.put("content_index", contentIndex);
            ObjectNode part = event.putObject("part");
            part.put("type", "output_text");
            part.put("text", contentBuffer.toString());
            part.putArray("annotations");
        });
    }

    private void emitMessageItemDone(List<ServerSentEvent<String>> results) {
        emitTypedEvent(results, "response.output_item.done", event -> {
            event.put("output_index", 0);
            ObjectNode item = event.putObject("item");
            item.put("type", "message");
            item.put("id", messageItemId);
            item.put("role", "assistant");
            item.put("status", "completed");
            ArrayNode content = item.putArray("content");
            ObjectNode textPart = content.addObject();
            textPart.put("type", "output_text");
            textPart.put("text", contentBuffer.toString());
            textPart.putArray("annotations");
        });
    }

    private void emitFunctionCallDone(List<ServerSentEvent<String>> results) {
        String toolItemId = activeToolItemId;
        int toolOutputIndex = activeToolOutputIndex;
        String toolName = activeToolName;
        String arguments = toolArgumentsBuffer.toString();
        emitTypedEvent(results, "response.function_call_arguments.done", event -> {
            event.put("item_id", toolItemId);
            event.put("output_index", toolOutputIndex);
            event.put("name", toolName);
            event.put("arguments", arguments);
        });
    }

    private void emitFunctionCallItemDone(List<ServerSentEvent<String>> results, int toolOutputIndex,
                                          String toolItemId, String toolName, String arguments) {
        emitTypedEvent(results, "response.output_item.done", event -> {
            event.put("output_index", toolOutputIndex);
            ObjectNode item = event.putObject("item");
            item.put("type", "function_call");
            item.put("id", toolItemId);
            item.put("call_id", toolItemId);
            item.put("name", toolName);
            item.put("arguments", arguments);
            item.put("status", "completed");
        });
    }

    private void emitCompleted(List<ServerSentEvent<String>> results) {
        if (!streamCompleted.compareAndSet(false, true)) {
            return;
        }
        ObjectNode response = buildResponseShell(finishStatus.get());
        response.set("output", buildFinalOutput());
        ObjectNode usage = response.putObject("usage");
        usage.put("input_tokens", inputTokens.get());
        usage.put("output_tokens", outputTokens.get());
        usage.put("total_tokens", inputTokens.get() + outputTokens.get());

        emitTypedEvent(results, "response.completed", event -> {
            event.set("response", response);
        });
    }

    private ArrayNode buildFinalOutput() {
        ArrayNode output = objectMapper.createArrayNode();
        if (activeToolItemId != null || toolArgumentsBuffer.length() > 0) {
            ObjectNode functionCall = output.addObject();
            functionCall.put("type", "function_call");
            functionCall.put("id", activeToolItemId != null ? activeToolItemId : "call_0");
            functionCall.put("call_id", functionCall.get("id").asText());
            functionCall.put("name", activeToolName != null ? activeToolName : "");
            functionCall.put("arguments", toolArgumentsBuffer.toString());
            functionCall.put("status", "completed");
            return output;
        }
        if (!contentBuffer.isEmpty() || messageItemOpened.get()) {
            ObjectNode message = output.addObject();
            message.put("type", "message");
            message.put("id", messageItemId);
            message.put("role", "assistant");
            message.put("status", "completed");
            ArrayNode content = message.putArray("content");
            ObjectNode textPart = content.addObject();
            textPart.put("type", "output_text");
            textPart.put("text", contentBuffer.toString());
            textPart.putArray("annotations");
        }
        return output;
    }

    private ObjectNode buildResponseShell(String status) {
        ObjectNode response = objectMapper.createObjectNode();
        response.put("id", responseId);
        response.put("object", "response");
        response.put("created_at", System.currentTimeMillis() / 1000);
        response.put("status", status);
        response.put("model", model);
        response.putNull("error");
        response.putNull("incomplete_details");
        response.putNull("usage");
        response.putArray("output");
        return response;
    }

    @FunctionalInterface
    private interface EventCustomizer {
        void customize(ObjectNode event);
    }

    private void emitTypedEvent(List<ServerSentEvent<String>> results, String type, EventCustomizer customizer) {
        ObjectNode event = objectMapper.createObjectNode();
        event.put("type", type);
        event.put("sequence_number", sequenceNumber.getAndIncrement());
        customizer.customize(event);
        try {
            String json = objectMapper.writeValueAsString(event);
            results.add(ServerSentEvent.<String>builder()
                    .event(type)
                    .data(json)
                    .build());
        } catch (Exception e) {
            log.error("Failed to serialize Responses API event {}", type, e);
        }
    }
}
