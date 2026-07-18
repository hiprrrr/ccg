package com.padb.ccg.proxy;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Anthropic SSE 事件流聚合器：把 message_start / content_block_* / message_delta
 * 事件序列还原为完整的 Anthropic message JSON，用于 stream:false 的非流式响应。
 * 上游非流式调用本身也会把完整 message 包装成同样的事件序列（见
 * HttpPassthroughProxyHandler.wrapAnthropicMessageAsSse），因此两种上游路径都能聚合。
 */
public final class AnthropicSseAggregator {

    private static final Logger log = LoggerFactory.getLogger(AnthropicSseAggregator.class);

    private final ObjectMapper objectMapper;

    /** message_start 中的 message 壳（id/model/role/usage） */
    private ObjectNode messageShell;
    /** 按 index 存放的内容块起始对象（content_block_start 的 content_block） */
    private final List<ObjectNode> blocks = new ArrayList<>();
    /** 按 index 存放的文本增量累积（text_delta / thinking_delta） */
    private final List<StringBuilder> textAccumulators = new ArrayList<>();
    /** 按 index 存放的 tool_use 输入 JSON 增量累积（input_json_delta） */
    private final List<StringBuilder> inputJsonAccumulators = new ArrayList<>();

    private String stopReason;
    private String stopSequence;
    private int outputTokens;

    public AnthropicSseAggregator(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    /** 消费一个 SSE data JSON；无法解析或无关类型直接忽略。 */
    public void consume(String data) {
        final JsonNode event;
        try {
            event = objectMapper.readTree(data);
        } catch (Exception e) {
            log.warn("Skipping unparseable Anthropic SSE data: {}", e.getMessage());
            return;
        }
        switch (event.path("type").asText()) {
            case "message_start" -> {
                JsonNode message = event.path("message");
                if (message instanceof ObjectNode msg) {
                    messageShell = msg;
                }
            }
            case "content_block_start" -> {
                int index = event.path("index").asInt(blocks.size());
                JsonNode block = event.path("content_block");
                ensureSlot(index);
                blocks.set(index, block instanceof ObjectNode b ? b : objectMapper.createObjectNode());
            }
            case "content_block_delta" -> {
                int index = event.path("index").asInt(0);
                ensureSlot(index);
                JsonNode delta = event.path("delta");
                switch (delta.path("type").asText()) {
                    case "text_delta" -> textAccumulators.get(index).append(delta.path("text").asText(""));
                    case "thinking_delta" -> textAccumulators.get(index).append(delta.path("thinking").asText(""));
                    case "input_json_delta" ->
                            inputJsonAccumulators.get(index).append(delta.path("partial_json").asText(""));
                    default -> { }
                }
            }
            case "message_delta" -> {
                JsonNode delta = event.path("delta");
                if (delta.hasNonNull("stop_reason")) {
                    stopReason = delta.get("stop_reason").asText();
                }
                stopSequence = delta.hasNonNull("stop_sequence") ? delta.get("stop_sequence").asText() : null;
                if (event.path("usage").hasNonNull("output_tokens")) {
                    outputTokens = event.path("usage").get("output_tokens").asInt();
                }
            }
            default -> { }
        }
    }

    /** 构建完整 Anthropic message JSON。 */
    public ObjectNode buildMessage() {
        ObjectNode message = messageShell != null ? messageShell : objectMapper.createObjectNode();
        message.put("type", "message");
        if (!message.has("role")) {
            message.put("role", "assistant");
        }

        ArrayNode content = message.putArray("content");
        for (int i = 0; i < blocks.size(); i++) {
            ObjectNode block = blocks.get(i);
            if (block == null) {
                continue;
            }
            String type = block.path("type").asText();
            if ("text".equals(type)) {
                block.put("text", textAccumulators.get(i).toString());
            } else if ("thinking".equals(type)) {
                block.put("thinking", textAccumulators.get(i).toString());
            } else if ("tool_use".equals(type)) {
                block.set("input", parseToolInput(inputJsonAccumulators.get(i).toString()));
            }
            content.add(block);
        }

        if (stopReason != null) {
            message.put("stop_reason", stopReason);
        }
        if (stopSequence != null) {
            message.put("stop_sequence", stopSequence);
        }
        ObjectNode usage = message.withObject("/usage");
        if (outputTokens > 0) {
            usage.put("output_tokens", outputTokens);
        }
        return message;
    }

    /** tool_use 的 input 字段：增量 JSON 解析失败时降级为空对象，保证结构合法。 */
    private JsonNode parseToolInput(String partialJson) {
        if (partialJson.isBlank()) {
            return objectMapper.createObjectNode();
        }
        try {
            return objectMapper.readTree(partialJson);
        } catch (Exception e) {
            log.warn("Failed to parse accumulated tool_use input JSON: {}", e.getMessage());
            return objectMapper.createObjectNode();
        }
    }

    /** 按 index 扩容三个并行列表。 */
    private void ensureSlot(int index) {
        while (blocks.size() <= index) {
            blocks.add(null);
            textAccumulators.add(new StringBuilder());
            inputJsonAccumulators.add(new StringBuilder());
        }
    }
}
