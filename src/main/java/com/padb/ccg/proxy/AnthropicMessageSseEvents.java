package com.padb.ccg.proxy;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.springframework.http.codec.ServerSentEvent;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * 把完整的 Anthropic message JSON 展开为 Anthropic SSE 事件序列。
 *
 * <p>用于上游只提供非流式接口、而下游协议要求 Anthropic SSE 事件的场景：
 * Bedrock 上 Anthropic 族模型（{@code anthropic.claude-*} 等）的 {@code InvokeModel}
 * 返回的是完整 message JSON，网关需要还原成
 * {@code message_start → content_block_* → message_delta → message_stop} 事件流，
 * 才能被 {@link AnthropicSseAggregator}（stream:false 聚合）与
 * {@link OpenAiSseConverter}（Anthropic→OpenAI）消费。</p>
 *
 * <p>事件结构遵循 Anthropic Messages 流式协议：
 * {@code message_delta.delta.stop_reason} 与 {@code message_delta.usage.output_tokens} 均为顶层字段。</p>
 */
final class AnthropicMessageSseEvents {

    private AnthropicMessageSseEvents() {
    }

    /**
     * 将完整 Anthropic message JSON 展开为 Anthropic SSE 事件序列。
     *
     * @param mapper     Jackson 映射器
     * @param messageJson 上游返回的完整 message JSON
     * @param userModel  面向用户的模型名称（写入 message_start.message.model）
     * @return Anthropic SSE 事件列表
     */
    static List<ServerSentEvent<String>> fromMessage(ObjectMapper mapper, String messageJson, String userModel)
            throws JsonProcessingException {
        JsonNode root = mapper.readTree(messageJson);
        List<ServerSentEvent<String>> events = new ArrayList<>();

        String messageId = root.path("id").asText("msg_" + UUID.randomUUID().toString().replace("-", ""));
        int inputTokens = root.path("usage").path("input_tokens").asInt(0);
        int outputTokens = root.path("usage").path("output_tokens").asInt(0);

        ObjectNode messageStart = mapper.createObjectNode();
        messageStart.put("type", "message_start");
        ObjectNode message = messageStart.putObject("message");
        message.put("id", messageId);
        message.put("type", "message");
        message.put("role", "assistant");
        message.put("model", userModel);
        message.putArray("content");
        ObjectNode startUsage = message.putObject("usage");
        startUsage.put("input_tokens", inputTokens);
        startUsage.put("output_tokens", 0);
        events.add(event("message_start", messageStart));

        JsonNode content = root.path("content");
        int blockIndex = 0;
        if (content.isArray()) {
            for (JsonNode block : content) {
                if (appendBlock(mapper, events, block, blockIndex)) {
                    blockIndex++;
                }
            }
        }

        ObjectNode messageDelta = mapper.createObjectNode();
        messageDelta.put("type", "message_delta");
        ObjectNode delta = messageDelta.putObject("delta");
        delta.put("stop_reason", root.path("stop_reason").asText("end_turn"));
        delta.putNull("stop_sequence");
        ObjectNode deltaUsage = messageDelta.putObject("usage");
        deltaUsage.put("output_tokens", outputTokens);
        events.add(event("message_delta", messageDelta));

        ObjectNode messageStop = mapper.createObjectNode();
        messageStop.put("type", "message_stop");
        events.add(event("message_stop", messageStop));
        return events;
    }

    /**
     * 展开单个 content block；返回 false 表示该块类型无需展开（如 server_tool_use 等扩展块）。
     */
    private static boolean appendBlock(ObjectMapper mapper, List<ServerSentEvent<String>> events,
                                       JsonNode block, int index) {
        if (!block.isObject()) {
            return false;
        }
        String type = block.path("type").asText("");
        switch (type) {
            case "text" -> {
                appendTextBlock(mapper, events, index, block.path("text").asText(""));
                return true;
            }
            case "thinking" -> {
                appendTextBlock(mapper, events, index, block.path("thinking").asText(""), "thinking_delta", "thinking");
                return true;
            }
            case "tool_use" -> {
                appendToolUseBlock(mapper, events, index, block);
                return true;
            }
            default -> {
                return false;
            }
        }
    }

    /** text / thinking 块：content_block_start → text_delta → content_block_stop。 */
    private static void appendTextBlock(ObjectMapper mapper, List<ServerSentEvent<String>> events,
                                        int index, String text) {
        appendTextBlock(mapper, events, index, text, "text_delta", "text");
    }

    private static void appendTextBlock(ObjectMapper mapper, List<ServerSentEvent<String>> events,
                                        int index, String text, String deltaType, String contentField) {
        ObjectNode blockStart = mapper.createObjectNode();
        blockStart.put("type", "content_block_start");
        blockStart.put("index", index);
        ObjectNode startBlock = blockStart.putObject("content_block");
        startBlock.put("type", contentField);
        startBlock.put(contentField, "");
        events.add(event("content_block_start", blockStart));

        if (text != null && !text.isEmpty()) {
            ObjectNode delta = mapper.createObjectNode();
            delta.put("type", "content_block_delta");
            delta.put("index", index);
            ObjectNode deltaBlock = delta.putObject("delta");
            deltaBlock.put("type", deltaType);
            deltaBlock.put(contentField, text);
            events.add(event("content_block_delta", delta));
        }

        events.add(blockStop(mapper, index));
    }

    /** tool_use 块：content_block_start → input_json_delta（整段 input）→ content_block_stop。 */
    private static void appendToolUseBlock(ObjectMapper mapper, List<ServerSentEvent<String>> events,
                                            int index, JsonNode block) {
        ObjectNode blockStart = mapper.createObjectNode();
        blockStart.put("type", "content_block_start");
        blockStart.put("index", index);
        ObjectNode startBlock = blockStart.putObject("content_block");
        startBlock.put("type", "tool_use");
        startBlock.put("id", block.path("id").asText(""));
        startBlock.put("name", block.path("name").asText(""));
        // input 由随后的 input_json_delta 补全，这里先放空对象占位
        startBlock.putObject("input");
        events.add(event("content_block_start", blockStart));

        JsonNode input = block.get("input");
        String partialJson = input == null || input.isNull() ? "{}" : input.toString();
        ObjectNode delta = mapper.createObjectNode();
        delta.put("type", "content_block_delta");
        delta.put("index", index);
        ObjectNode deltaBlock = delta.putObject("delta");
        deltaBlock.put("type", "input_json_delta");
        deltaBlock.put("partial_json", partialJson);
        events.add(event("content_block_delta", delta));

        events.add(blockStop(mapper, index));
    }

    private static ServerSentEvent<String> blockStop(ObjectMapper mapper, int index) {
        ObjectNode blockStop = mapper.createObjectNode();
        blockStop.put("type", "content_block_stop");
        blockStop.put("index", index);
        return event("content_block_stop", blockStop);
    }

    private static ServerSentEvent<String> event(String name, ObjectNode data) {
        return ServerSentEvent.<String>builder().event(name).data(data.toString()).build();
    }
}
