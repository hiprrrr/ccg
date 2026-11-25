package com.padb.ccg.proxy;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.codec.ServerSentEvent;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * 将 Amazon Nova 的响应转换为 Anthropic SSE 事件。
 *
 * <p>Nova 的流式事件（与 Converse 事件同构）：</p>
 * <pre>
 * {"contentBlockStart":{"start":{"text":""},"contentBlockIndex":0}}
 * {"contentBlockStart":{"start":{"toolUse":{"toolUseId":"...","name":"..."}},"contentBlockIndex":1}}
 * {"contentBlockDelta":{"delta":{"text":"..."},"contentBlockIndex":0}}
 * {"contentBlockDelta":{"delta":{"toolUse":{"input":"{...}"}},"contentBlockIndex":1}}
 * {"contentBlockStop":{"contentBlockIndex":0}}
 * {"messageStop":{"stopReason":"end_turn"}}
 * {"metadata":{"usage":{"inputTokens":10,"outputTokens":20,"totalTokens":30}}}
 * </pre>
 *
 * <p>转换结果为 Anthropic 事件序列：
 * {@code message_start → content_block_start / content_block_delta / content_block_stop
 * → message_delta → message_stop}。</p>
 *
 * <p>Nova 的 token 用量只出现在流末的 metadata 块里，因此 {@code message_start} 中的
 * {@code input_tokens} 为 0，真实用量放在 {@code message_delta.usage} 中。</p>
 */
class NovaSseConverter {

    private static final Logger log = LoggerFactory.getLogger(NovaSseConverter.class);

    /** 转换器状态机阶段。TAIL_PENDING 表示已收到 messageStop、等待流末 metadata 补齐用量后收尾。 */
    private enum State { INITIAL, STREAMING, TAIL_PENDING, DONE }

    private final ObjectMapper mapper;
    private final String messageId;
    private final String model;

    private State state = State.INITIAL;
    /** messageStop 携带的 stopReason，待 metadata 到达后随 message_delta 一起发出。 */
    private String pendingStopReason;
    private int inputTokens;
    private int outputTokens;
    private int nextBlockIndex;
    /** 已打开的内容块索引 → 块类型（text / tool_use），用于收尾时补发 content_block_stop。 */
    private final Map<Integer, String> openBlocks = new LinkedHashMap<>();
    /** 已发出 content_block_start 的索引，避免同一块重复发起点事件。 */
    private final Map<Integer, Boolean> startedBlocks = new LinkedHashMap<>();
    /** 已结束的内容块索引。 */
    private final Map<Integer, Boolean> stoppedBlocks = new LinkedHashMap<>();

    NovaSseConverter(ObjectMapper mapper, String model) {
        this.mapper = mapper;
        this.messageId = "msg_" + UUID.randomUUID().toString().replace("-", "");
        this.model = model;
    }

    int getInputTokens() {
        return inputTokens;
    }

    int getOutputTokens() {
        return outputTokens;
    }

    /**
     * 将单个 Nova 流式事件块转换为零个或多个 Anthropic SSE 事件。
     */
    List<ServerSentEvent<String>> convert(String rawChunk) {
        if (rawChunk == null || rawChunk.isBlank()) {
            return List.of();
        }
        final JsonNode root;
        try {
            root = mapper.readTree(rawChunk);
        } catch (JsonProcessingException e) {
            log.warn("Failed to parse Bedrock Nova chunk as JSON: {}", e.getMessage());
            return List.of();
        }
        // Nova 的 metadata 块在 messageStop 之后到达，DONE 状态下仍需提取 token 用量
        if (root.has("metadata")) {
            extractUsage(root.at("/metadata/usage"));
            if (state == State.TAIL_PENDING) {
                List<ServerSentEvent<String>> tail = flushPendingTail();
                return tail;
            }
        }
        if (state == State.DONE || state == State.TAIL_PENDING) {
            return List.of();
        }
        if (!root.isObject()) {
            return List.of();
        }

        List<ServerSentEvent<String>> events = new ArrayList<>();
        if (state == State.INITIAL) {
            events.add(buildMessageStartEvent());
            state = State.STREAMING;
        }

        if (root.has("contentBlockStart")) {
            appendBlockStart(events, root.at("/contentBlockStart"));
        } else if (root.has("contentBlockDelta")) {
            appendBlockDelta(events, root.at("/contentBlockDelta"));
        } else if (root.has("contentBlockStop")) {
            appendBlockStop(events, root.at("/contentBlockStop"));
        } else if (root.has("messageStop")) {
            // 先关闭内容块，message_delta/message_stop 挂起等 metadata 补齐用量后统一发出
            closeOpenBlocks(events);
            pendingStopReason = mapStopReason(root.at("/messageStop/stopReason").asText("end_turn"));
            state = State.TAIL_PENDING;
        }
        return events;
    }

    /**
     * 关流时补齐未发送的结束事件（部分上游不会发送 messageStop）。
     */
    List<ServerSentEvent<String>> endStreamIfOpen() {
        if (state == State.DONE) {
            return List.of();
        }
        List<ServerSentEvent<String>> events = new ArrayList<>();
        if (state == State.INITIAL) {
            events.add(buildMessageStartEvent());
        }
        if (state == State.STREAMING) {
            closeOpenBlocks(events);
            pendingStopReason = "end_turn";
        }
        events.addAll(flushPendingTail());
        return events;
    }

    /** 发出挂起的 message_delta + message_stop，并进入 DONE。 */
    private List<ServerSentEvent<String>> flushPendingTail() {
        List<ServerSentEvent<String>> events = new ArrayList<>();
        addStopEvents(events, pendingStopReason != null ? pendingStopReason : "end_turn");
        state = State.DONE;
        return events;
    }

    /** contentBlockStart：开一个 text 或 tool_use 内容块。 */
    private void appendBlockStart(List<ServerSentEvent<String>> events, JsonNode blockStart) {
        int index = blockStart.path("contentBlockIndex").asInt(nextBlockIndex);
        JsonNode start = blockStart.path("start");
        JsonNode toolUse = start.path("toolUse");
        ObjectNode event = mapper.createObjectNode();
        event.put("type", "content_block_start");
        event.put("index", index);
        ObjectNode contentBlock = event.putObject("content_block");
        if (!toolUse.isMissingNode()) {
            contentBlock.put("type", "tool_use");
            contentBlock.put("id", toolUse.path("toolUseId").asText(""));
            contentBlock.put("name", toolUse.path("name").asText(""));
            contentBlock.putObject("input");
            openBlocks.put(index, "tool_use");
        } else {
            contentBlock.put("type", "text");
            contentBlock.put("text", "");
            openBlocks.put(index, "text");
        }
        startedBlocks.put(index, Boolean.TRUE);
        events.add(toSse("content_block_start", event));
    }

    /** contentBlockDelta：文本增量或工具入参增量。 */
    private void appendBlockDelta(List<ServerSentEvent<String>> events, JsonNode blockDelta) {
        int index = blockDelta.path("contentBlockIndex").asInt(0);
        JsonNode delta = blockDelta.path("delta");
        JsonNode text = delta.path("text");
        JsonNode toolInput = delta.at("/toolUse/input");

        if (startedBlocks.get(index) == null) {
            // 上游未发 contentBlockStart 就发增量：补一个起点事件，保证 Anthropic 事件序列完整
            ObjectNode blockStart = mapper.createObjectNode();
            blockStart.put("type", "content_block_start");
            blockStart.put("index", index);
            ObjectNode contentBlock = blockStart.putObject("content_block");
            if (!toolInput.isMissingNode()) {
                contentBlock.put("type", "tool_use");
                contentBlock.put("id", "");
                contentBlock.put("name", "");
                contentBlock.putObject("input");
                openBlocks.put(index, "tool_use");
            } else {
                contentBlock.put("type", "text");
                contentBlock.put("text", "");
                openBlocks.put(index, "text");
            }
            startedBlocks.put(index, Boolean.TRUE);
            events.add(toSse("content_block_start", blockStart));
        }

        if (!toolInput.isMissingNode() && !toolInput.isNull()) {
            ObjectNode event = mapper.createObjectNode();
            event.put("type", "content_block_delta");
            event.put("index", index);
            ObjectNode deltaObj = event.putObject("delta");
            deltaObj.put("type", "input_json_delta");
            deltaObj.put("partial_json", toolInput.asText(""));
            events.add(toSse("content_block_delta", event));
            return;
        }
        if (!text.isMissingNode() && !text.isNull() && !text.asText("").isEmpty()) {
            ObjectNode event = mapper.createObjectNode();
            event.put("type", "content_block_delta");
            event.put("index", index);
            ObjectNode deltaObj = event.putObject("delta");
            deltaObj.put("type", "text_delta");
            deltaObj.put("text", text.asText(""));
            events.add(toSse("content_block_delta", event));
        }
    }

    /** contentBlockStop：关闭一个内容块。 */
    private void appendBlockStop(List<ServerSentEvent<String>> events, JsonNode blockStop) {
        int index = blockStop.path("contentBlockIndex").asInt(0);
        if (stoppedBlocks.get(index) != null) {
            return;
        }
        stoppedBlocks.put(index, Boolean.TRUE);
        openBlocks.remove(index);
        ObjectNode event = mapper.createObjectNode();
        event.put("type", "content_block_stop");
        event.put("index", index);
        events.add(toSse("content_block_stop", event));
    }

    /** 关闭所有仍打开的内容块。 */
    private void closeOpenBlocks(List<ServerSentEvent<String>> events) {
        for (Integer index : new ArrayList<>(openBlocks.keySet())) {
            ObjectNode event = mapper.createObjectNode();
            event.put("type", "content_block_stop");
            event.put("index", index);
            events.add(toSse("content_block_stop", event));
        }
        openBlocks.clear();
    }

    /** 补发 message_delta + message_stop。 */
    private void addStopEvents(List<ServerSentEvent<String>> events, String stopReason) {
        ObjectNode messageDelta = mapper.createObjectNode();
        messageDelta.put("type", "message_delta");
        ObjectNode delta = messageDelta.putObject("delta");
        delta.put("stop_reason", stopReason);
        delta.putNull("stop_sequence");
        ObjectNode usage = messageDelta.putObject("usage");
        usage.put("output_tokens", outputTokens);
        if (inputTokens > 0) {
            usage.put("input_tokens", inputTokens);
        }
        events.add(toSse("message_delta", messageDelta));

        ObjectNode messageStop = mapper.createObjectNode();
        messageStop.put("type", "message_stop");
        events.add(toSse("message_stop", messageStop));
    }

    /** message_start 事件。 */
    private ServerSentEvent<String> buildMessageStartEvent() {
        ObjectNode event = mapper.createObjectNode();
        event.put("type", "message_start");
        ObjectNode message = event.putObject("message");
        message.put("id", messageId);
        message.put("type", "message");
        message.put("role", "assistant");
        message.put("model", model);
        message.putArray("content");
        ObjectNode usage = message.putObject("usage");
        usage.put("input_tokens", inputTokens);
        usage.put("output_tokens", 0);
        return toSse("message_start", event);
    }

    /** 提取 Nova usage（inputTokens / outputTokens）。 */
    private void extractUsage(JsonNode usage) {
        if (usage.isMissingNode() || usage.isNull()) {
            return;
        }
        int in = usage.path("inputTokens").asInt(0);
        int out = usage.path("outputTokens").asInt(0);
        if (in > 0) {
            inputTokens = in;
        }
        if (out > 0) {
            outputTokens = out;
        }
    }

    /** Nova stopReason → Anthropic stop_reason。 */
    private static String mapStopReason(String reason) {
        return switch (reason) {
            case "max_tokens" -> "max_tokens";
            case "stop_sequence" -> "stop_sequence";
            case "tool_use" -> "tool_use";
            default -> "end_turn";
        };
    }

    private ServerSentEvent<String> toSse(String name, ObjectNode data) {
        return ServerSentEvent.<String>builder().event(name).data(data.toString()).build();
    }

    /**
     * 将 Nova 的非流式响应转换为 Anthropic message JSON。
     *
     * <p>Nova 非流式响应形如
     * {@code {"output":{"message":{"role":"assistant","content":[{"text":"..."}]}},"stopReason":"end_turn",
     * "usage":{"inputTokens":10,"outputTokens":20}}}。</p>
     *
     * @param mapper     Jackson 映射器
     * @param rawResponse Nova 原始响应 JSON
     * @param userModel  面向用户的模型名称
     * @return Anthropic message JSON
     */
    static String toAnthropicMessage(ObjectMapper mapper, String rawResponse, String userModel)
            throws JsonProcessingException {
        JsonNode root = mapper.readTree(rawResponse);
        ObjectNode message = mapper.createObjectNode();
        message.put("id", root.path("output").path("message").path("id")
                .asText("msg_" + UUID.randomUUID().toString().replace("-", "")));
        message.put("type", "message");
        message.put("role", "assistant");
        message.put("model", userModel);

        ArrayNode content = message.putArray("content");
        JsonNode blocks = root.at("/output/message/content");
        if (blocks.isArray()) {
            for (JsonNode block : blocks) {
                if (!block.isObject()) {
                    continue;
                }
                JsonNode text = block.path("text");
                if (!text.isMissingNode()) {
                    ObjectNode textBlock = content.addObject();
                    textBlock.put("type", "text");
                    textBlock.put("text", text.asText(""));
                    continue;
                }
                JsonNode toolUse = block.path("toolUse");
                if (!toolUse.isMissingNode()) {
                    ObjectNode toolUseBlock = content.addObject();
                    toolUseBlock.put("type", "tool_use");
                    toolUseBlock.put("id", toolUse.path("toolUseId").asText(""));
                    toolUseBlock.put("name", toolUse.path("name").asText(""));
                    JsonNode input = toolUse.path("input");
                    if (input.isObject()) {
                        toolUseBlock.set("input", input.deepCopy());
                    } else {
                        toolUseBlock.putObject("input");
                    }
                }
            }
        }

        message.put("stop_reason", mapStopReason(root.path("stopReason").asText("end_turn")));
        ObjectNode usage = message.putObject("usage");
        usage.put("input_tokens", root.at("/usage/inputTokens").asInt(0));
        usage.put("output_tokens", root.at("/usage/outputTokens").asInt(0));
        return mapper.writeValueAsString(message);
    }
}
