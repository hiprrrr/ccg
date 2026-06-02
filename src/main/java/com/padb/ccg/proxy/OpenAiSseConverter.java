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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * 将 Bedrock/Anthropic SSE 事件流转换为 OpenAI 兼容格式。
 *
 * OpenAI 格式示例：
 * - data: {"id":"chatcmpl-xxx","object":"chat.completion.chunk","choices":[{"index":0,"delta":{"content":"Hi"}}]}
 * - data: [DONE]
 */
public class OpenAiSseConverter {

    private static final Logger log = LoggerFactory.getLogger(OpenAiSseConverter.class);

    private final ObjectMapper objectMapper;
    private final String model;
    private final String responseId;

    // 累积的内容，用于流式 delta
    private final StringBuilder contentBuffer = new StringBuilder();
    private final AtomicInteger inputTokens = new AtomicInteger(0);
    private final AtomicInteger outputTokens = new AtomicInteger(0);
    private final AtomicReference<String> finishReason = new AtomicReference<>(null);

    // 用于追踪 tool_calls
    private final List<ToolCallBuffer> toolCallBuffers = new ArrayList<>();

    public OpenAiSseConverter(ObjectMapper objectMapper, String model) {
        this.objectMapper = objectMapper;
        this.model = model;
        this.responseId = "chatcmpl-" + UUID.randomUUID().toString().substring(0, 8);
    }

    /**
     * 转换 Anthropic SSE 事件为 OpenAI 格式
     */
    public List<ServerSentEvent<String>> convert(String anthropicData) {
        List<ServerSentEvent<String>> results = new ArrayList<>();

        try {
            JsonNode root = objectMapper.readTree(anthropicData);
            String eventType = root.has("type") ? root.get("type").asText() : null;

            if ("message_start".equals(eventType)) {
                handleEventStart(root, results);
            } else if ("content_block_start".equals(eventType)) {
                handleContentBlockStart(root, results);
            } else if ("content_block_delta".equals(eventType)) {
                handleContentBlockDelta(root, results);
            } else if ("content_block_stop".equals(eventType)) {
                // 内容块结束，无需特殊处理
            } else if ("message_delta".equals(eventType)) {
                handleMessageDelta(root, results);
            } else if ("message_stop".equals(eventType)) {
                handleMessageStop(results);
            } else {
                // 尝试直接解析为 Bedrock 原始响应
                handleBedrockRaw(anthropicData, results);
            }
        } catch (Exception e) {
            log.warn("Failed to parse Anthropic event: {}", e.getMessage());
            // 尝试作为 Bedrock 原始格式处理
            handleBedrockRaw(anthropicData, results);
        }

        return results;
    }

    private void handleEventStart(JsonNode root, List<ServerSentEvent<String>> results) {
        JsonNode message = root.path("message");
        if (message.has("usage")) {
            JsonNode usage = message.get("usage");
            if (usage.has("input_tokens")) {
                inputTokens.set(usage.get("input_tokens").asInt());
            }
        }
        // OpenAI 不在流开始时发送事件，只是初始化
    }

    private void handleContentBlockStart(JsonNode root, List<ServerSentEvent<String>> results) {
        int index = root.path("index").asInt();
        JsonNode contentBlock = root.path("content_block");

        String type = contentBlock.path("type").asText();

        if ("tool_use".equals(type)) {
            // 工具调用开始
            ToolCallBuffer buffer = new ToolCallBuffer();
            buffer.index = index;
            buffer.id = contentBlock.path("id").asText("call_" + index);
            buffer.type = "function";
            buffer.name = contentBlock.path("name").asText("");
            buffer.arguments = new StringBuilder();

            // 确保列表足够长
            while (toolCallBuffers.size() <= index) {
                toolCallBuffers.add(null);
            }
            toolCallBuffers.set(index, buffer);

            // 发送 tool_calls delta
            ObjectNode delta = objectMapper.createObjectNode();
            ArrayNode toolCalls = delta.putArray("tool_calls");
            ObjectNode tc = toolCalls.addObject();
            tc.put("index", index);
            ObjectNode function = tc.putObject("function");
            function.put("name", buffer.name);
            function.put("arguments", "");
            tc.put("id", buffer.id);
            tc.put("type", "function");

            emitDelta(results, delta);
        }
        // text 类型的内容块开始时不需要特殊处理
    }

    private void handleContentBlockDelta(JsonNode root, List<ServerSentEvent<String>> results) {
        int index = root.path("index").asInt();
        JsonNode delta = root.path("delta");

        String deltaType = delta.path("type").asText();

        if ("text_delta".equals(deltaType)) {
            String text = delta.path("text").asText("");
            if (!text.isEmpty()) {
                contentBuffer.append(text);
                ObjectNode deltaNode = objectMapper.createObjectNode();
                deltaNode.put("content", text);
                emitDelta(results, deltaNode);
            }
        } else if ("input_json_delta".equals(deltaType)) {
            // 工具参数增量
            String partialJson = delta.path("partial_json").asText("");
            if (!partialJson.isEmpty() && index < toolCallBuffers.size()) {
                ToolCallBuffer buffer = toolCallBuffers.get(index);
                if (buffer != null) {
                    buffer.arguments.append(partialJson);

                    ObjectNode deltaNode = objectMapper.createObjectNode();
                    ArrayNode toolCalls = deltaNode.putArray("tool_calls");
                    ObjectNode tc = toolCalls.addObject();
                    tc.put("index", index);
                    ObjectNode function = tc.putObject("function");
                    function.put("arguments", partialJson);

                    emitDelta(results, deltaNode);
                }
            }
        }
    }

    private void handleMessageDelta(JsonNode root, List<ServerSentEvent<String>> results) {
        JsonNode delta = root.path("delta");
        JsonNode usage = root.path("usage");

        if (delta.has("stop_reason")) {
            String stopReason = delta.get("stop_reason").asText();
            finishReason.set(convertStopReason(stopReason));
        }

        if (usage.has("output_tokens")) {
            outputTokens.set(usage.get("output_tokens").asInt());
        }
    }

    private void handleMessageStop(List<ServerSentEvent<String>> results) {
        // 发送最终的 finish 事件
        ObjectNode deltaNode = objectMapper.createObjectNode();
        if (finishReason.get() != null) {
            deltaNode.put("content", (String) null);
        }
        emitDeltaWithFinish(results, deltaNode, finishReason.get());

        // 发送 [DONE]
        results.add(ServerSentEvent.<String>builder()
                .data("[DONE]")
                .build());
    }

    /**
     * 处理 Bedrock 原始响应（非 Anthropic SSE 格式）
     */
    private void handleBedrockRaw(String data, List<ServerSentEvent<String>> results) {
        try {
            JsonNode root = objectMapper.readTree(data);

            // 检查是否是完成响应
            if (root.has("completion")) {
                // Claude 2 风格
                String completion = root.get("completion").asText("");
                if (!completion.isEmpty()) {
                    contentBuffer.append(completion);
                    ObjectNode deltaNode = objectMapper.createObjectNode();
                    deltaNode.put("content", completion);
                    emitDelta(results, deltaNode);
                }
            } else if (root.has("content")) {
                // 可能是完整消息
                JsonNode content = root.get("content");
                if (content.isArray()) {
                    for (JsonNode block : content) {
                        String type = block.path("type").asText();
                        if ("text".equals(type)) {
                            String text = block.path("text").asText("");
                            if (!text.isEmpty()) {
                                contentBuffer.append(text);
                                ObjectNode deltaNode = objectMapper.createObjectNode();
                                deltaNode.put("content", text);
                                emitDelta(results, deltaNode);
                            }
                        }
                    }
                }
            } else if (root.has("generation")) {
                // Titan 等模型
                String generation = root.get("generation").asText("");
                if (!generation.isEmpty()) {
                    contentBuffer.append(generation);
                    ObjectNode deltaNode = objectMapper.createObjectNode();
                    deltaNode.put("content", generation);
                    emitDelta(results, deltaNode);
                }
            }

            // 提取 usage
            if (root.has("usage")) {
                JsonNode usage = root.get("usage");
                if (usage.has("input_tokens")) {
                    inputTokens.set(usage.get("input_tokens").asInt());
                }
                if (usage.has("output_tokens")) {
                    outputTokens.set(usage.get("output_tokens").asInt());
                }
            }

            // 检查是否是最终响应（包含 stop_reason）
            if (root.has("stop_reason")) {
                String stopReason = root.get("stop_reason").asText();
                finishReason.set(convertStopReason(stopReason));
                emitDeltaWithFinish(results, objectMapper.createObjectNode(), finishReason.get());
                results.add(ServerSentEvent.<String>builder()
                        .data("[DONE]")
                        .build());
            }

        } catch (Exception e) {
            log.warn("Failed to parse Bedrock raw response: {}", e.getMessage());
        }
    }

    private void emitDelta(List<ServerSentEvent<String>> results, ObjectNode delta) {
        emitDeltaWithFinish(results, delta, null);
    }

    private void emitDeltaWithFinish(List<ServerSentEvent<String>> results, ObjectNode delta, String finishReason) {
        ObjectNode chunk = objectMapper.createObjectNode();
        chunk.put("id", responseId);
        chunk.put("object", "chat.completion.chunk");
        chunk.put("created", System.currentTimeMillis() / 1000);
        chunk.put("model", model);

        ArrayNode choices = chunk.putArray("choices");
        ObjectNode choice = choices.addObject();
        choice.put("index", 0);
        choice.set("delta", delta);
        if (finishReason != null) {
            choice.put("finish_reason", finishReason);
        } else {
            choice.putNull("finish_reason");
        }

        try {
            String json = objectMapper.writeValueAsString(chunk);
            results.add(ServerSentEvent.<String>builder()
                    .data(json)
                    .build());
        } catch (Exception e) {
            log.error("Failed to serialize OpenAI chunk", e);
        }
    }

    /**
     * 转换 Anthropic stop_reason 到 OpenAI finish_reason
     */
    private String convertStopReason(String anthropicStopReason) {
        if (anthropicStopReason == null) return null;
        return switch (anthropicStopReason) {
            case "end_turn" -> "stop";
            case "max_tokens" -> "length";
            case "stop_sequence" -> "stop";
            case "tool_use" -> "tool_calls";
            default -> "stop";
        };
    }

    /**
     * 流结束时发送 [DONE]（如果还没有发送）
     */
    public List<ServerSentEvent<String>> endStreamIfOpen() {
        List<ServerSentEvent<String>> results = new ArrayList<>();
        if (finishReason.get() == null) {
            // 强制结束
            ObjectNode deltaNode = objectMapper.createObjectNode();
            emitDeltaWithFinish(results, deltaNode, "stop");
            results.add(ServerSentEvent.<String>builder()
                    .data("[DONE]")
                    .build());
        }
        return results;
    }

    public int getInputTokens() {
        return inputTokens.get();
    }

    public int getOutputTokens() {
        return outputTokens.get();
    }

    /**
     * 工具调用缓冲区
     */
    private static class ToolCallBuffer {
        int index;
        String id;
        String type;
        String name;
        StringBuilder arguments;
    }
}
