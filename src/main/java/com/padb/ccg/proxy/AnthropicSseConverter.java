package com.padb.ccg.proxy;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.codec.ServerSentEvent;

import java.util.*;

/**
 * 将 OpenAI 格式的聊天补全 SSE 块（来自 Bedrock）转换为 Anthropic 格式的 SSE 事件。
 *
 * <p>每个请求需创建一个实例（有状态）。状态机跟踪消息生命周期：
 * INITIAL → STREAMING → DONE，确保正确的 event: 类型和顺序。</p>
 */
class AnthropicSseConverter {

    private static final Logger log = LoggerFactory.getLogger(AnthropicSseConverter.class);
    /** 与 {@link BedrockProperties#toolCallTraceEnabled()} 配套：仅打工具链相关行，便于单独配置日志级别 */
    private static final Logger toolTrace = LoggerFactory.getLogger("com.padb.ccg.proxy.ToolCallTrace");

    /** 转换器状态 */
    private enum State { INITIAL, STREAMING, DONE }

    private final ObjectMapper mapper;
    private final String messageId;
    private final String model;
    /** 与网关 request id 对齐，写入 ToolCallTrace 日志 */
    private final String traceId;
    /** 是否输出工具调用转换细节（OpenAI chunk → Anthropic SSE） */
    private final boolean logToolCalls;
    /** 工具排查日志中单段字符串最大长度 */
    private final int toolLogMaxChars;

    private State state = State.INITIAL;
    /** 是否在 {@link #endStreamIfOpen()} 中因一直停留在 INITIAL 而补发了合成 Anthropic 尾包（供网关排查日志使用）。 */
    private boolean syntheticInitialTailUsed;
    private int inputTokens;
    private int outputTokens;
    private int nextBlockIndex;
    private Integer textBlockIndex;
    private boolean textBlockStopped;
    private final Map<Integer, ToolState> toolStates = new LinkedHashMap<>();

    /** OpenAI tool_calls 流式片段的累积状态。 */
    private static final class ToolState {
        private Integer blockIndex;
        private String id;
        private String name;
        private final StringBuilder pendingArguments = new StringBuilder();
        private boolean started;
        private boolean stopped;
        /** 已向上游发出的 input_json_delta 中 partial_json 累计字符数（关流时对照 pendingRemain） */
        private int emittedJsonDeltaChars;
    }

    /**
     * @param mapper Jackson ObjectMapper，用于 JSON 解析和序列化
     * @param model  面向用户的模型名称（如 claude-sonnet-4-6），非 Bedrock 内部 ID
     */
    AnthropicSseConverter(ObjectMapper mapper, String model) {
        this(mapper, model, "-", false, 4096);
    }

    /**
     * @param traceId       与 ProxyService request id 对齐
     * @param logToolCalls  是否写入 {@code ToolCallTrace} 日志
     * @param toolLogMaxChars 单条工具排查日志最大字符数
     */
    AnthropicSseConverter(ObjectMapper mapper, String model, String traceId, boolean logToolCalls, int toolLogMaxChars) {
        this.mapper = mapper;
        this.messageId = "msg_" + UUID.randomUUID().toString().replace("-", "");
        this.model = model;
        this.traceId = traceId == null || traceId.isBlank() ? "-" : traceId;
        this.logToolCalls = logToolCalls;
        this.toolLogMaxChars = Math.max(256, toolLogMaxChars);
    }

    /** 获取从 Bedrock 响应中提取的输入 token 数 */
    int getInputTokens() { return inputTokens; }

    /** 获取从 Bedrock 响应中提取的输出 token 数 */
    int getOutputTokens() { return outputTokens; }

    /** 是否因从未识别到 OpenAI 风格 chunk 而在关流时走了「INITIAL 合成尾包」分支（排查空回复 / 中断用）。 */
    boolean isSyntheticInitialTailUsed() {
        return syntheticInitialTailUsed;
    }

    /** 当前 Anthropic 转换状态机阶段，便于与 Bedrock 原始 chunk 对照。 */
    String debugLifecycleState() {
        return state.name();
    }

    /**
     * 关流时汇总各 OpenAI tool_calls 索引上的累积参数增量与未刷出缓冲，供 ToolTrace 单行诊断。
     */
    String summarizeToolConversionAtStreamEnd() {
        if (toolStates.isEmpty()) {
            return "";
        }
        StringBuilder b = new StringBuilder();
        for (Map.Entry<Integer, ToolState> e : toolStates.entrySet()) {
            ToolState s = e.getValue();
            if (b.length() > 0) {
                b.append(" | ");
            }
            b.append("callIx=").append(e.getKey())
                    .append(",name=").append(s.name != null ? s.name : "?")
                    .append(",id=").append(s.id != null ? s.id : "?")
                    .append(",started=").append(s.started)
                    .append(",argDeltaChars=").append(s.emittedJsonDeltaChars)
                    .append(",pendingChars=").append(s.pendingArguments.length());
        }
        return b.toString();
    }

    /**
     * 将单个 Bedrock 原始 JSON 块转换为一或多个 Anthropic SSE 事件
     *
     * @param rawChunk Bedrock onChunk 回调的原始 JSON 字符串
     * @return Anthropic SSE 事件列表（可能为空）
     */
    List<ServerSentEvent<String>> convert(String rawChunk) {
        JsonNode root;
        try {
            root = mapper.readTree(rawChunk);
        } catch (JsonProcessingException e) {
            log.warn("Failed to parse Bedrock chunk as JSON: {} chunkPreview={}", e.getMessage(),
                    truncateForDiag(rawChunk, 800));
            return Collections.emptyList();
        }

        switch (state) {
            case INITIAL:
                return handleInitial(root);
            case STREAMING:
                return handleStreaming(root);
            case DONE:
                return Collections.emptyList();
            default:
                return Collections.emptyList();
        }
    }

    /**
     * 将非流式 OpenAI Chat Completions 响应一次性包装成 Anthropic SSE 事件序列。
     * 用于下游模型流式接口不稳定但非流式 InvokeModel 可用的场景。
     */
    List<ServerSentEvent<String>> convertCompleteMessage(String rawResponse) {
        JsonNode root;
        try {
            root = mapper.readTree(rawResponse);
        } catch (JsonProcessingException e) {
            log.warn("Failed to parse Bedrock non-stream response as JSON: {} preview={}", e.getMessage(),
                    truncateForDiag(rawResponse, 1200));
            return Collections.emptyList();
        }

        JsonNode message = root.at("/choices/0/message");
        JsonNode finishReason = root.at("/choices/0/finish_reason");
        if (message.isMissingNode()) {
            return Collections.emptyList();
        }

        List<ServerSentEvent<String>> events = new ArrayList<>();
        events.add(ServerSentEvent.<String>builder()
                .event("message_start")
                .data(toJson(Map.of(
                        "type", "message_start",
                        "message", Map.of(
                                "id", messageId,
                                "type", "message",
                                "role", "assistant",
                                "content", Collections.emptyList(),
                                "model", model,
                                "usage", Map.of("input_tokens", 0)
                        )
                )))
                .build());

        ObjectNode syntheticDelta = mapper.createObjectNode();
        JsonNode content = message.get("content");
        if (content != null && !content.isNull() && !content.asText().isEmpty()) {
            syntheticDelta.put("content", content.asText());
        }
        JsonNode toolCalls = message.get("tool_calls");
        if (toolCalls != null && toolCalls.isArray()) {
            syntheticDelta.set("tool_calls", toolCalls);
        }
        appendDeltaEvents(events, syntheticDelta);

        extractMetrics(root);
        String reason = !finishReason.isMissingNode() && !finishReason.isNull()
                ? finishReason.asText()
                : (toolCalls != null && toolCalls.isArray() && !toolCalls.isEmpty() ? "tool_calls" : "stop");
        addStopEvents(events, reason);
        state = State.DONE;
        return events;
    }

    /**
     * 在 Bedrock 流已结束（SDK onComplete）时调用。
     * 部分模型不会在最后一个 chunk 中带 {@code choices[0].finish_reason}，
     * 若仍停留在 {@link State#STREAMING}，则按正常结束补发 Anthropic 结束事件，避免客户端读到半截 SSE。
     *
     * @return 需要补发的事件列表；已在 {@link State#DONE} 时返回空列表；
     *         若仍为 {@link State#INITIAL}（从未识别到 OpenAI 风格 chunk），则合成最小合法 Anthropic 序列，避免客户端读到空 SSE 而中断。
     */
    List<ServerSentEvent<String>> endStreamIfOpen() {
        if (state == State.DONE) {
            return Collections.emptyList();
        }
        if (state == State.INITIAL) {
            // 部分 Bedrock 模型（如部分 zai 流）chunk 形态与 choices[0].delta 不一致，整流无 message_start；
            // Claude Code 会当作异常中断，此处补发与「空回复」等价的完整生命周期事件。
            syntheticInitialTailUsed = true;
            log.warn("Bedrock stream ended in INITIAL state (no OpenAI-style chunks recognized); synthesizing minimal Anthropic SSE");
            List<ServerSentEvent<String>> events = new ArrayList<>();
            events.add(buildMessageStartEvent());
            addStopEvents(events, "stop");
            state = State.DONE;
            return events;
        }
        List<ServerSentEvent<String>> events = new ArrayList<>();
        addStopEvents(events, "stop");
        state = State.DONE;
        return events;
    }

    // ─── INITIAL 状态处理 ────────────────────────────────────────────

    /**
     * 处理首个有效块：发出 message_start，并按实际内容懒加载 text/tool content block。
     */
    private List<ServerSentEvent<String>> handleInitial(JsonNode root) {
        JsonNode delta = root.at("/choices/0/delta");
        JsonNode finishReason = root.at("/choices/0/finish_reason");

        // 无效块（缺失 delta 或 finish_reason）
        if (delta.isMissingNode() && finishReason.isMissingNode()) {
            return Collections.emptyList();
        }

        List<ServerSentEvent<String>> events = new ArrayList<>();

        // 1. message_start
        events.add(buildMessageStartEvent());

        appendDeltaEvents(events, delta);

        // 如果首个块就已完成（finish_reason 非空），发出结束事件
        if (!finishReason.isMissingNode() && !finishReason.isNull()) {
            extractMetrics(root);
            addStopEvents(events, finishReason.asText());
            state = State.DONE;
        } else {
            state = State.STREAMING;
        }

        return events;
    }

    // ─── STREAMING 状态处理 ──────────────────────────────────────────

    /**
     * 处理流式块：有内容则发 content_block_delta，
     * 有 finish_reason 则发 content_block_stop + message_delta + message_stop
     */
    private List<ServerSentEvent<String>> handleStreaming(JsonNode root) {
        JsonNode delta = root.at("/choices/0/delta");
        JsonNode finishReason = root.at("/choices/0/finish_reason");

        // 同一 SSE 帧内可能同时携带 delta 与 finish_reason（含 tool_calls 尾片 + tool_calls 结束）。
        // 必须先合并 delta，再 addStopEvents；若先结束则从未执行 appendToolCallDelta，会丢掉参数尾片，
        // Claude Code 侧表现为 tool input 不完整 → Invalid tool parameters（例如缺 file_path / command）。
        List<ServerSentEvent<String>> events = new ArrayList<>();
        if (!delta.isMissingNode()) {
            appendDeltaEvents(events, delta);
        }

        if (!finishReason.isMissingNode() && !finishReason.isNull()) {
            extractMetrics(root);
            addStopEvents(events, finishReason.asText());
            state = State.DONE;
            return events;
        }

        if (!events.isEmpty()) {
            return events;
        }

        return Collections.emptyList();
    }

    // ─── 辅助方法 ────────────────────────────────────────────────────

    /** 构建 Anthropic {@code message_start} SSE 事件（与 handleInitial / 合成尾包共用）。 */
    private ServerSentEvent<String> buildMessageStartEvent() {
        return ServerSentEvent.<String>builder()
                .event("message_start")
                .data(toJson(Map.of(
                        "type", "message_start",
                        "message", Map.of(
                                "id", messageId,
                                "type", "message",
                                "role", "assistant",
                                "content", Collections.emptyList(),
                                "model", model,
                                "usage", Map.of("input_tokens", 0)
                        )
                )))
                .build();
    }

    /** 从 OpenAI delta 中追加 Anthropic 文本或工具调用事件。 */
    private void appendDeltaEvents(List<ServerSentEvent<String>> events, JsonNode delta) {
        JsonNode contentNode = delta.at("/content");
        if (!contentNode.isMissingNode() && !contentNode.asText().isEmpty()) {
            ensureTextBlockStarted(events);
            events.add(buildTextDelta(contentNode.asText()));
        }

        JsonNode toolCalls = delta.get("tool_calls");
        if (toolCalls != null && toolCalls.isArray()) {
            for (JsonNode toolCall : toolCalls) {
                appendToolCallDelta(events, toolCall);
            }
        }
    }

    /** 确保文本 content block 已打开。 */
    private void ensureTextBlockStarted(List<ServerSentEvent<String>> events) {
        if (textBlockIndex != null) {
            return;
        }
        textBlockIndex = nextBlockIndex++;
        events.add(ServerSentEvent.<String>builder()
                .event("content_block_start")
                .data(toJson(Map.of(
                        "type", "content_block_start",
                        "index", textBlockIndex,
                        "content_block", Map.of("type", "text", "text", "")
                )))
                .build());
    }

    /** 构建文本 content_block_delta SSE 事件。 */
    private ServerSentEvent<String> buildTextDelta(String text) {
        return ServerSentEvent.<String>builder()
                .event("content_block_delta")
                .data(toJson(Map.of(
                        "type", "content_block_delta",
                        "index", textBlockIndex,
                        "delta", Map.of("type", "text_delta", "text", text)
                )))
                .build();
    }

    /** 将 OpenAI tool_calls 增量转换为 Anthropic tool_use content block。 */
    private void appendToolCallDelta(List<ServerSentEvent<String>> events, JsonNode toolCall) {
        int callIndex = toolCall.path("index").asInt(toolStates.size());
        ToolState state = toolStates.computeIfAbsent(callIndex, k -> new ToolState());
        if (logToolCalls) {
            try {
                toolTrace.info("ToolTrace OPENAI_TOOL_CALL_DELTA traceId={} callIndex={} payload={}",
                        traceId, callIndex, truncateForDiag(mapper.writeValueAsString(toolCall), toolLogMaxChars));
            } catch (JsonProcessingException e) {
                toolTrace.info("ToolTrace OPENAI_TOOL_CALL_DELTA traceId={} callIndex={} payload=(serialize_failed)",
                        traceId, callIndex);
            }
        }
        if (toolCall.hasNonNull("id")) {
            state.id = toolCall.get("id").asText();
        }
        JsonNode function = toolCall.get("function");
        if (function != null && function.isObject()) {
            if (function.hasNonNull("name")) {
                state.name = function.get("name").asText();
            }
            if (function.has("arguments") && !function.get("arguments").isNull()) {
                appendOpenAiArgumentsFragment(state.pendingArguments, function.get("arguments"));
            }
        }

        if (!state.started && state.name != null && !state.name.isBlank()) {
            startToolBlock(events, state);
        }
        if (state.started && state.pendingArguments.length() > 0) {
            String partial = state.pendingArguments.toString();
            state.pendingArguments.setLength(0);
            state.emittedJsonDeltaChars += partial.length();
            if (logToolCalls) {
                toolTrace.info("ToolTrace ANTHROPIC_INPUT_JSON_DELTA traceId={} blockIndex={} partialChars={} preview={}",
                        traceId, state.blockIndex, partial.length(), truncateForDiag(partial, Math.min(toolLogMaxChars, 800)));
            }
            events.add(ServerSentEvent.<String>builder()
                    .event("content_block_delta")
                    .data(toJson(Map.of(
                            "type", "content_block_delta",
                            "index", state.blockIndex,
                            "delta", Map.of("type", "input_json_delta", "partial_json", partial)
                    )))
                    .build());
        }
    }

    /**
     * 将 OpenAI {@code function.arguments} 增量写入缓冲区：多数供应商为 JSON 字符串片段，
     * 部分模型（如部分 Bedrock 端点）会给出已解析的 JSON 对象；对 Object/Array 使用 writeValueAsString，
     * 避免 {@link JsonNode#asText()} 对对象返回空串导致 Claude Code 报 Invalid tool parameters。
     */
    private void appendOpenAiArgumentsFragment(StringBuilder target, JsonNode argNode) {
        if (argNode == null || argNode.isNull()) {
            return;
        }
        if (argNode.isTextual()) {
            target.append(argNode.asText());
            return;
        }
        if (argNode.isNumber() || argNode.isBoolean()) {
            target.append(argNode.asText());
            return;
        }
        if (argNode.isObject() || argNode.isArray()) {
            try {
                target.append(mapper.writeValueAsString(argNode));
            } catch (JsonProcessingException e) {
                log.warn("Failed to serialize tool function.arguments as JSON: {}", e.getMessage());
            }
        }
    }

    /** 打开 Anthropic tool_use content block。 */
    private void startToolBlock(List<ServerSentEvent<String>> events, ToolState state) {
        state.blockIndex = nextBlockIndex++;
        state.started = true;
        Map<String, Object> contentBlock = new LinkedHashMap<>();
        contentBlock.put("type", "tool_use");
        contentBlock.put("id", state.id != null && !state.id.isBlank()
                ? state.id
                : "toolu_" + UUID.randomUUID().toString().replace("-", ""));
        contentBlock.put("name", state.name);
        contentBlock.put("input", Collections.emptyMap());

        events.add(ServerSentEvent.<String>builder()
                .event("content_block_start")
                .data(toJson(Map.of(
                        "type", "content_block_start",
                        "index", state.blockIndex,
                        "content_block", contentBlock
                )))
                .build());
        if (logToolCalls) {
            toolTrace.info("ToolTrace ANTHROPIC_TOOL_USE_START traceId={} blockIndex={} id={} name={}",
                    traceId, state.blockIndex, contentBlock.get("id"), state.name);
        }
    }

    /** 追加结束事件序列：content_block_stop → message_delta → message_stop */
    private void addStopEvents(List<ServerSentEvent<String>> events, String finishReason) {
        closeOpenBlocks(events);

        // message_delta
        Map<String, Object> usage = new LinkedHashMap<>();
        usage.put("output_tokens", outputTokens);
        if (inputTokens > 0) usage.put("input_tokens", inputTokens);

        // Map.of 不允许 null；Anthropic 协议中 stop_sequence 可为 JSON null
        Map<String, Object> deltaObj = new LinkedHashMap<>();
        deltaObj.put("stop_reason", mapStopReason(finishReason));
        deltaObj.put("stop_sequence", null);
        Map<String, Object> messageDelta = new LinkedHashMap<>();
        messageDelta.put("type", "message_delta");
        messageDelta.put("delta", deltaObj);
        messageDelta.put("usage", usage);

        events.add(ServerSentEvent.<String>builder()
                .event("message_delta")
                .data(toJson(messageDelta))
                .build());

        // message_stop
        events.add(ServerSentEvent.<String>builder()
                .event("message_stop")
                .data(toJson(Map.of("type", "message_stop")))
                .build());
    }

    /** 关闭所有已经打开的 content block，避免客户端看到半截工具调用或文本块。 */
    private void closeOpenBlocks(List<ServerSentEvent<String>> events) {
        // Claude Code 对 Anthropic SSE 生命周期较严格；即使下游返回空内容，也补一个空 text block。
        if (nextBlockIndex == 0) {
            ensureTextBlockStarted(events);
        }
        if (textBlockIndex != null && !textBlockStopped) {
            events.add(buildContentBlockStop(textBlockIndex));
            textBlockStopped = true;
        }
        for (ToolState state : toolStates.values()) {
            if (!state.started && state.name != null && !state.name.isBlank()) {
                startToolBlock(events, state);
            }
            if (state.started && state.pendingArguments.length() > 0) {
                String partial = state.pendingArguments.toString();
                state.pendingArguments.setLength(0);
                state.emittedJsonDeltaChars += partial.length();
                if (logToolCalls) {
                    toolTrace.warn("ToolTrace ANTHROPIC_INPUT_JSON_FLUSH traceId={} blockIndex={} partialChars={} preview={} (flush_at_stream_end)",
                            traceId, state.blockIndex, partial.length(), truncateForDiag(partial, Math.min(toolLogMaxChars, 800)));
                }
                events.add(ServerSentEvent.<String>builder()
                        .event("content_block_delta")
                        .data(toJson(Map.of(
                                "type", "content_block_delta",
                                "index", state.blockIndex,
                                "delta", Map.of("type", "input_json_delta", "partial_json", partial)
                        )))
                        .build());
            }
            if (state.started && !state.stopped) {
                events.add(buildContentBlockStop(state.blockIndex));
                state.stopped = true;
            }
        }
    }

    /** 构建 content_block_stop 事件。 */
    private ServerSentEvent<String> buildContentBlockStop(int index) {
        return ServerSentEvent.<String>builder()
                .event("content_block_stop")
                .data(toJson(Map.of(
                        "type", "content_block_stop",
                        "index", index
                )))
                .build();
    }

    /** 从 amazon-bedrock-invocationMetrics 提取 token 统计 */
    private void extractMetrics(JsonNode root) {
        JsonNode metrics = root.at("/amazon-bedrock-invocationMetrics");
        if (!metrics.isMissingNode()) {
            if (metrics.has("inputTokenCount")) {
                this.inputTokens = metrics.get("inputTokenCount").asInt();
            }
            if (metrics.has("outputTokenCount")) {
                this.outputTokens = metrics.get("outputTokenCount").asInt();
            }
        }
    }

    /** OpenAI finish_reason → Anthropic stop_reason 映射 */
    private static String mapStopReason(String reason) {
        return switch (reason) {
            case "stop" -> "end_turn";
            case "length" -> "max_tokens";
            case "tool_calls", "function_call" -> "tool_use";
            default       -> reason; // content_filter, tool_calls 等原样透传
        };
    }

    /** 将单行长文本截断，仅用于排查日志，避免异常大的 chunk 撑爆日志后端。 */
    private static String truncateForDiag(String s, int maxChars) {
        if (s == null) {
            return "";
        }
        String oneLine = s.replace('\r', ' ').replace('\n', ' ');
        if (oneLine.length() <= maxChars) {
            return oneLine;
        }
        return oneLine.substring(0, maxChars) + "...(totalChars=" + s.length() + ")";
    }

    /** 将对象序列化为 JSON 字符串 */
    private String toJson(Object obj) {
        try {
            return mapper.writeValueAsString(obj);
        } catch (Exception e) {
            log.warn("Failed to serialize Anthropic event", e);
            return "{}";
        }
    }
}
