package com.padb.ccg.proxy;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * 在调用 Bedrock {@code InvokeModelWithResponseStream} 前对请求体做规范化。
 * <p>
 * Claude Code 等客户端可能在 JSON 根级携带 {@code metadata}（如 {@code user_id}），
 * 其取值若含路径、邮箱、中文等字符，会触发 Bedrock 400：
 * {@code Request metadata contains a value that violates the regular expression}。
 * 本类将 {@code metadata} 重写为仅含符合 Bedrock 约束的 {@code user_id}，
 * 取值来自网关注册的用户名（{@code x-api-key}），避免透传非法上游字段。
 * </p>
 * <p>
 * Claude Code 发出的 {@code tools} 条目可能省略 {@code type}，或在 {@code input_schema}
 * 中使用非标准 JSON Schema 类型 {@code custom}。对 {@code zai.*} 模型，Anthropic 风格且带
 * {@code input_schema} 的工具会映射为 OpenAI Chat Completions 的 {@code function} 工具，
 * 以便模型产生 {@code tool_calls} 增量而非在正文中伪造 {@code <tool_call>} 标记；其余路径仍按
 * Bedrock 要求补全 {@code type} 等字段。
 * </p>
 */
public final class BedrockInvokeBodyPreparer {

    private static final Logger log = LoggerFactory.getLogger(BedrockInvokeBodyPreparer.class);

    /** Bedrock 对 metadata.user_id 一类标识的常见允许字符集（保守子集） */
    private static final int MAX_USER_ID_LEN = 128;

    private BedrockInvokeBodyPreparer() {
    }

    /**
     * 解析请求体 JSON，移除顶层 {@code model} 字段并规范化 {@code tools} 与根级 {@code metadata}。
     *
     * @param mapper              Jackson 映射器
     * @param requestBody         请求体 JSON 文本（顶层 model 字段会在本方法内移除）
     * @param gatewayUsername     网关侧认证用户名（与 x-api-key 一致）
     * @return 可供 Bedrock 接受的请求体 JSON 字符串
     */
    public static String normalizeMetadata(ObjectMapper mapper, String requestBody, String gatewayUsername)
            throws JsonProcessingException {
        return normalizeMetadata(mapper, requestBody, gatewayUsername, null);
    }

    /**
     * 解析请求体 JSON，移除顶层 {@code model} 字段并规范化 {@code tools} 与根级 {@code metadata}。
     *
     * @param mapper              Jackson 映射器
     * @param requestBody         请求体 JSON 文本（顶层 model 字段会在本方法内移除）
     * @param gatewayUsername     网关侧认证用户名（与 x-api-key 一致）
     * @param bedrockModelId      Bedrock 实际模型 ID，用于处理供应商私有 schema 差异
     * @return 可供 Bedrock 接受的请求体 JSON 字符串
     */
    public static String normalizeMetadata(ObjectMapper mapper, String requestBody, String gatewayUsername,
                                           String bedrockModelId)
            throws JsonProcessingException {
        return normalizeMetadata(mapper, requestBody, gatewayUsername, bedrockModelId, true);
    }

    /**
     * 解析请求体 JSON，移除顶层 {@code model} 字段并规范化 {@code tools} 与根级 {@code metadata}。
     *
     * @param mapper              Jackson 映射器
     * @param requestBody         请求体 JSON 文本（顶层 model 字段会在本方法内移除）
     * @param gatewayUsername     网关侧认证用户名（与 x-api-key 一致）
     * @param bedrockModelId      Bedrock 实际模型 ID，用于处理供应商私有 schema 差异
     * @param toolsEnabled        当前模型是否允许工具调用；禁用时会移除工具定义与工具选择
     * @return 可供 Bedrock 接受的请求体 JSON 字符串
     */
    public static String normalizeMetadata(ObjectMapper mapper, String requestBody, String gatewayUsername,
                                           String bedrockModelId, boolean toolsEnabled)
            throws JsonProcessingException {
        return normalizeMetadata(mapper, requestBody, gatewayUsername, bedrockModelId, toolsEnabled, true);
    }

    /**
     * 解析请求体 JSON，移除顶层 {@code model} 字段并规范化 {@code tools} 与根级 {@code metadata}。
     *
     * @param mapper              Jackson 映射器
     * @param requestBody         请求体 JSON 文本（顶层 model 字段会在本方法内移除）
     * @param gatewayUsername     网关侧认证用户名（与 x-api-key 一致）
     * @param bedrockModelId      Bedrock 实际模型 ID，用于处理供应商私有 schema 差异
     * @param toolsEnabled        当前模型是否允许工具调用；禁用时会移除工具定义与工具选择
     * @param streamingEnabled    当前请求是否走 Bedrock 流式 API；非流式时必须移除 {@code stream}
     * @return 可供 Bedrock 接受的请求体 JSON 字符串
     */
    public static String normalizeMetadata(ObjectMapper mapper, String requestBody, String gatewayUsername,
                                           String bedrockModelId, boolean toolsEnabled, boolean streamingEnabled)
            throws JsonProcessingException {
        JsonNode root = mapper.readTree(requestBody);
        if (!root.isObject()) {
            return requestBody;
        }
        ObjectNode obj = (ObjectNode) root;
        // 移除顶层 model：Bedrock 不接受该字段，模型 ID 通过 InvokeModelRequest.modelId 单独指定
        obj.remove("model");
        boolean openAiChatShape = usesOpenAiChatShape(bedrockModelId);
        normalizeMessages(obj, openAiChatShape, toolsEnabled);
        if (toolsEnabled) {
            normalizeTools(obj, isZaiModel(bedrockModelId));
            if (openAiChatShape) {
                normalizeToolChoiceForOpenAi(obj);
            }
        } else {
            removeToolDefinitions(obj);
        }
        normalizeRootFields(obj, openAiChatShape, streamingEnabled);
        if (openAiChatShape) {
            stripUnsupportedAnthropicExtensions(obj);
        }
        ObjectNode meta = mapper.createObjectNode();
        meta.put("user_id", sanitizeUserId(gatewayUsername));
        obj.set("metadata", meta);
        return mapper.writeValueAsString(obj);
    }

    /**
     * Z.ai GLM 5 的 Bedrock Runtime 更接近 OpenAI Chat Completions。
     * 将 Claude Code 发来的 Anthropic 内容块转换为字符串、tool_calls 和 tool 消息。
     */
    private static void normalizeMessages(ObjectNode root, boolean useZaiShape, boolean toolsEnabled) {
        JsonNode messages = root.get("messages");
        if (!useZaiShape || messages == null || !messages.isArray()) {
            return;
        }

        ArrayNode normalized = root.arrayNode();
        JsonNode system = root.get("system");
        String systemText = contentToText(system);
        if (!systemText.isBlank()) {
            ObjectNode systemMsg = root.objectNode();
            systemMsg.put("role", "system");
            systemMsg.put("content", systemText);
            normalized.add(systemMsg);
        }

        for (JsonNode message : messages) {
            if (!message.isObject()) {
                continue;
            }
            String role = message.path("role").asText();
            JsonNode content = message.get("content");
            if (toolsEnabled && "user".equals(role)
                    && content != null && content.isArray() && hasBlockType(content, "tool_result")) {
                appendUserAndToolResultMessages(root, normalized, content);
            } else if (toolsEnabled && "assistant".equals(role)
                    && content != null && content.isArray() && hasBlockType(content, "tool_use")) {
                appendAssistantToolUseMessage(root, normalized, content);
            } else {
                appendPassthroughOpenAiMessage(root, normalized, (ObjectNode) message);
            }
        }

        sanitizeOpenAiMessages(normalized);
        root.set("messages", normalized);
        root.remove("system");
    }

    /**
     * 透传已是 OpenAI Chat 形态的 message，保留 {@code tool_call_id}、{@code tool_calls} 等 Bedrock 必填字段。
     * 通用 else 分支若只拷贝 role/content，会把 {@code role:tool} 的 {@code tool_call_id} 丢掉并触发 400。
     */
    private static void appendPassthroughOpenAiMessage(ObjectNode root, ArrayNode normalized, ObjectNode message) {
        String role = message.path("role").asText();
        JsonNode content = message.get("content");
        ObjectNode out = root.objectNode();
        out.put("role", role);
        if (content != null && content.isArray() && hasBlockType(content, "image")) {
            out.set("content", contentToOpenAiArray(root, content));
        } else {
            String text = contentToText(content);
            out.put("content", text);
        }
        if ("tool".equals(role)) {
            copyTextFieldIfPresent(message, out, "tool_call_id");
        } else if ("assistant".equals(role)) {
            JsonNode toolCalls = message.get("tool_calls");
            if (toolCalls != null && toolCalls.isArray() && !toolCalls.isEmpty()) {
                out.set("tool_calls", toolCalls.deepCopy());
                if (isBlankContent(out.get("content"))) {
                    out.remove("content");
                }
            }
        }
        normalized.add(out);
    }

    /** 将源对象上的字符串字段复制到目标（仅当非空白时写入）。 */
    private static void copyTextFieldIfPresent(ObjectNode from, ObjectNode to, String field) {
        String value = from.path(field).asText("");
        if (!value.isBlank()) {
            to.put(field, value);
        }
    }

    /** 当前模型不支持工具时，移除工具声明与工具选择，避免辅助模型卡在 unsupported tool schema。 */
    private static void removeToolDefinitions(ObjectNode root) {
        root.remove(List.of("tools", "tool_choice"));
    }

    /** 将包含 tool_result 的 Anthropic user message 拆成 OpenAI user/tool message。 */
    private static void appendUserAndToolResultMessages(ObjectNode root, ArrayNode normalized, JsonNode content) {
        StringBuilder userText = new StringBuilder();
        for (JsonNode block : content) {
            if (!block.isObject()) {
                continue;
            }
            String type = block.path("type").asText();
            if ("text".equals(type)) {
                appendText(userText, block.path("text").asText());
            } else if ("tool_result".equals(type)) {
                String toolCallId = resolveToolCallId(block);
                String resultText = contentToText(block.get("content"));
                if (toolCallId.isBlank()) {
                    // 无法关联 assistant.tool_calls 时降级为 user 文本，避免 Bedrock 400
                    appendText(userText, resultText);
                    continue;
                }
                if (userText.length() > 0) {
                    ObjectNode userMsg = root.objectNode();
                    userMsg.put("role", "user");
                    userMsg.put("content", userText.toString());
                    normalized.add(userMsg);
                    userText.setLength(0);
                }
                ObjectNode toolMsg = root.objectNode();
                toolMsg.put("role", "tool");
                toolMsg.put("tool_call_id", toolCallId);
                toolMsg.put("content", blankContentAsSpace(resultText));
                normalized.add(toolMsg);
            }
        }
        if (userText.length() > 0) {
            ObjectNode userMsg = root.objectNode();
            userMsg.put("role", "user");
            userMsg.put("content", userText.toString());
            normalized.add(userMsg);
        }
    }

    /**
     * 从 Anthropic {@code tool_result} 块解析 OpenAI {@code tool_call_id}。
     * 依次尝试 {@code tool_use_id}、{@code tool_call_id}、{@code id}，兼容不同客户端字段命名。
     */
    private static String resolveToolCallId(JsonNode toolResultBlock) {
        for (String field : List.of("tool_use_id", "tool_call_id", "id")) {
            String value = toolResultBlock.path(field).asText("");
            if (!value.isBlank()) {
                return value;
            }
        }
        return "";
    }

    /** OpenAI tool 消息要求 content 非空；空白结果用单空格占位。 */
    private static String blankContentAsSpace(String content) {
        return content == null || content.isBlank() ? " " : content;
    }

    /**
     * 清理 OpenAI messages：去掉缺 {@code tool_call_id} 的 tool 消息、空 user/system，
     * assistant 仅有 tool_calls 时移除空 content。
     */
    private static void sanitizeOpenAiMessages(ArrayNode messages) {
        for (int i = messages.size() - 1; i >= 0; i--) {
            JsonNode msg = messages.get(i);
            if (!(msg instanceof ObjectNode obj)) {
                messages.remove(i);
                continue;
            }
            String role = obj.path("role").asText();
            boolean hasToolCalls = obj.has("tool_calls")
                    && obj.get("tool_calls").isArray()
                    && !obj.get("tool_calls").isEmpty();

            if ("assistant".equals(role)) {
                if (hasToolCalls && isBlankContent(obj.get("content"))) {
                    obj.remove("content");
                } else if (!hasToolCalls && isBlankContent(obj.get("content"))) {
                    messages.remove(i);
                }
            } else if ("user".equals(role) || "system".equals(role)) {
                if (isBlankContent(obj.get("content"))) {
                    messages.remove(i);
                }
            } else if ("tool".equals(role)) {
                if (obj.path("tool_call_id").asText("").isBlank()) {
                    messages.remove(i);
                } else if (isBlankContent(obj.get("content"))) {
                    obj.put("content", " ");
                }
            }
        }
    }

    /** 判断 OpenAI message content 是否为空（null、空白字符串或空数组）。 */
    private static boolean isBlankContent(JsonNode content) {
        if (content == null || content.isNull()) {
            return true;
        }
        if (content.isTextual()) {
            return content.asText().isBlank();
        }
        if (content.isArray()) {
            return content.isEmpty();
        }
        return false;
    }

    /** 将 Anthropic assistant tool_use content block 转为 OpenAI assistant.tool_calls。 */
    private static void appendAssistantToolUseMessage(ObjectNode root, ArrayNode normalized, JsonNode content) {
        ObjectNode assistantMsg = root.objectNode();
        assistantMsg.put("role", "assistant");
        StringBuilder text = new StringBuilder();
        ArrayNode toolCalls = root.arrayNode();
        for (JsonNode block : content) {
            if (!block.isObject()) {
                continue;
            }
            String type = block.path("type").asText();
            if ("text".equals(type)) {
                appendText(text, block.path("text").asText());
            } else if ("tool_use".equals(type)) {
                ObjectNode call = root.objectNode();
                call.put("id", block.path("id").asText());
                call.put("type", "function");
                ObjectNode function = root.objectNode();
                function.put("name", block.path("name").asText());
                JsonNode input = block.get("input");
                function.put("arguments", input != null && !input.isNull() ? input.toString() : "{}");
                call.set("function", function);
                toolCalls.add(call);
            }
        }
        assistantMsg.put("content", text.toString());
        if (!toolCalls.isEmpty()) {
            assistantMsg.set("tool_calls", toolCalls);
        }
        normalized.add(assistantMsg);
    }

    /** 清理 OpenAI/ChatCompletions 风格 Bedrock 模型不需要的 Anthropic 专用根字段，并做常见字段映射。 */
    private static void normalizeRootFields(ObjectNode root, boolean openAiChatShape, boolean streamingEnabled) {
        if (!streamingEnabled) {
            root.remove("stream");
        }
        if (!openAiChatShape) {
            return;
        }
        root.remove(List.of(
                "anthropic_version",
                "anthropic_beta",
                "betas",
                "thinking",
                "output_config",
                "cache_control",
                "mcp_servers",
                "service_tier",
                "container",
                "context_management",
                "stream"
        ));
        JsonNode stopSequences = root.get("stop_sequences");
        if (stopSequences != null && !root.has("stop")) {
            root.set("stop", stopSequences);
        }
        root.remove("stop_sequences");
    }

    /**
     * 内置 Claude Code experimental beta 过滤器，等效于网关侧强制开启
     * {@code CLAUDE_CODE_DISABLE_EXPERIMENTAL_BETAS=1} 的安全子集。
     * 这些字段属于 Anthropic/Claude Code 扩展，OpenAI Chat 风格 Bedrock 模型不接受。
     */
    private static void stripUnsupportedAnthropicExtensions(JsonNode node) {
        if (node == null) {
            return;
        }
        if (node.isObject()) {
            ObjectNode obj = (ObjectNode) node;
            obj.remove(List.of(
                    "cache_control",
                    "anthropic_beta",
                    "betas",
                    "mcp_servers",
                    "context_management",
                    "defer_loading",
                    "eager_input_streaming",
                    "tool_schema",
                    "input_examples"
            ));
            obj.fields().forEachRemaining(entry -> stripUnsupportedAnthropicExtensions(entry.getValue()));
        } else if (node.isArray()) {
            node.forEach(BedrockInvokeBodyPreparer::stripUnsupportedAnthropicExtensions);
        }
    }

    /** 判断内容数组中是否含指定类型的 Anthropic content block。 */
    private static boolean hasBlockType(JsonNode content, String type) {
        for (JsonNode block : content) {
            if (block.isObject() && type.equals(block.path("type").asText())) {
                return true;
            }
        }
        return false;
    }

    /** 将 Anthropic 字符串/内容块数组压缩为普通文本。 */
    private static String contentToText(JsonNode content) {
        if (content == null || content.isNull()) {
            return "";
        }
        if (content.isTextual()) {
            return content.asText();
        }
        if (content.isArray()) {
            StringBuilder sb = new StringBuilder();
            for (JsonNode block : content) {
                if (block.isTextual()) {
                    appendText(sb, block.asText());
                } else if (block.isObject()) {
                    String type = block.path("type").asText();
                    if ("text".equals(type)) {
                        appendText(sb, block.path("text").asText());
                    } else {
                        JsonNode nested = block.get("content");
                        if (nested != null) {
                            appendText(sb, contentToText(nested));
                        }
                    }
                }
            }
            return sb.toString();
        }
        return content.toString();
    }

    /** 追加文本块，多个块之间用换行隔开，避免黏连。 */
    private static void appendText(StringBuilder sb, String text) {
        if (text == null || text.isBlank()) {
            return;
        }
        if (sb.length() > 0) {
            sb.append('\n');
        }
        sb.append(text);
    }

    /**
     * 将 Anthropic content 块数组翻译为 OpenAI 多模态 content 数组。
     * - text 块 → {"type":"text","text":"..."}
     * - image 块 (source.type=base64) → {"type":"image_url","image_url":{"url":"data:<media_type>;base64,<data>"}}
     * - image 块 (source.type=url)     → {"type":"image_url","image_url":{"url":"<url>"}}
     * 其他块类型原样透传，交由上游校验。
     */
    private static ArrayNode contentToOpenAiArray(ObjectNode root, JsonNode content) {
        ArrayNode out = root.arrayNode();
        for (JsonNode block : content) {
            if (!block.isObject()) {
                continue;
            }
            String type = block.path("type").asText();
            if ("text".equals(type)) {
                ObjectNode textBlock = root.objectNode();
                textBlock.put("type", "text");
                textBlock.put("text", block.path("text").asText(""));
                out.add(textBlock);
            } else if ("image".equals(type)) {
                JsonNode source = block.path("source");
                String sourceType = source.path("type").asText();
                ObjectNode imageBlock = root.objectNode();
                imageBlock.put("type", "image_url");
                ObjectNode imageUrl = imageBlock.putObject("image_url");
                if ("base64".equals(sourceType)) {
                    String mediaType = source.path("media_type").asText("image/png");
                    String data = source.path("data").asText("");
                    imageUrl.put("url", "data:" + mediaType + ";base64," + data);
                } else if ("url".equals(sourceType)) {
                    imageUrl.put("url", source.path("url").asText(""));
                } else {
                    // 未知 source 类型：原样透传 image 块，让上游报错暴露问题
                    out.add(block);
                    continue;
                }
                out.add(imageBlock);
            } else {
                out.add(block);
            }
        }
        return out;
    }

    /**
     * Bedrock 要求 {@code tools[]} 每个元素都有 {@code type}；Anthropic 直连允许省略。
     * 对 {@code zai.*}：优先将 name+input_schema 转为 OpenAI {@code function}；其余再补 {@code custom} 或透传。
     */
    private static void normalizeTools(ObjectNode root, boolean useZaiToolShape) {
        JsonNode tools = root.get("tools");
        if (tools == null || !tools.isArray()) {
            return;
        }
        ArrayNode normalized = root.arrayNode();
        for (JsonNode t : tools) {
            if (!t.isObject()) {
                normalized.add(t);
                continue;
            }
            ObjectNode tool = (ObjectNode) t;
            normalizeToolInputSchema(tool);
            // 过滤掉 Bedrock 不支持的托管工具类型（如 web_search_20250305, text_editor_20250728 等）
            // Bedrock 只接受 custom 或 function 类型
            JsonNode typeNode = tool.get("type");
            if (typeNode != null && typeNode.isTextual()) {
                String toolType = typeNode.asText();
                // 托管工具类型格式：xxx_YYYYMMDD（如 web_search_20250305）
                // 这些是 Anthropic 专属工具，Bedrock 不支持
                if (!toolType.isEmpty() && !"custom".equals(toolType) && !"function".equals(toolType)) {
                    // 检查是否是托管工具类型（包含日期格式的后缀）
                    if (toolType.matches(".*_\\d{8}$")) {
                        // 跳过 Bedrock 不支持的托管工具，并记 warn 让丢弃可见
                        log.warn("Dropping unsupported Anthropic hosted tool type='{}' name='{}' (Bedrock only accepts custom/function)",
                                toolType, tool.path("name").asText(""));
                        continue;
                    }
                }
            }
            // Z.ai：Anthropic 风格（缺省/ custom 类型 + name + input_schema）→ OpenAI function，才能触发真正的 tool_calls 流
            if (useZaiToolShape && shouldEmitOpenAiFunctionToolForZai(tool)) {
                normalized.add(toOpenAiFunctionTool(tool));
                continue;
            }
            boolean missing = !tool.has("type") || typeNode == null || typeNode.isNull();
            boolean blankText = typeNode != null && typeNode.isTextual() && typeNode.asText().isBlank();
            if (missing || blankText) {
                tool.put("type", "custom");
            }
            if (useZaiToolShape && "custom".equals(tool.path("type").asText())) {
                normalized.add(toZaiCustomTool(tool));
            } else {
                // Bedrock 不接受 Claude Code 工具定义上的 custom 附加配置，工具 schema 已在 input_schema 表达
                tool.remove("custom");
                normalized.add(tool);
            }
        }
        root.set("tools", normalized);
    }

    /**
     * 判断是否为「应转成 OpenAI function 工具」的 Anthropic 风格定义：有 name、有 object 型 input_schema，
     * 且 {@code type} 缺省、空白或为 {@code custom}（托管类工具会带显式 vendor type，不在此列）。
     */
    private static boolean shouldEmitOpenAiFunctionToolForZai(ObjectNode tool) {
        if (!tool.has("name") || tool.path("name").asText().isBlank()) {
            return false;
        }
        if (!tool.has("input_schema") || !tool.get("input_schema").isObject()) {
            return false;
        }
        String declared = tool.path("type").asText("");
        return declared.isBlank() || "custom".equals(declared);
    }

    /**
     * Anthropic {@code tools[]} 单条 → OpenAI Chat Completions {@code {"type":"function","function":{...}}}。
     */
    private static ObjectNode toOpenAiFunctionTool(ObjectNode tool) {
        ObjectNode out = tool.objectNode();
        out.put("type", "function");
        ObjectNode function = tool.objectNode();
        function.put("name", tool.path("name").asText());
        JsonNode desc = tool.get("description");
        if (desc != null && desc.isTextual() && !desc.asText().isBlank()) {
            function.put("description", desc.asText());
        }
        ObjectNode parameters = (ObjectNode) tool.get("input_schema").deepCopy();
        normalizeJsonSchemaCustomTypesToObject(parameters);
        function.set("parameters", parameters);
        out.set("function", function);
        return out;
    }

    /**
     * 将 Anthropic 风格的 {@code tool_choice} 对象映射为 OpenAI Chat Completions 接受的字符串或对象。
     */
    private static void normalizeToolChoiceForOpenAi(ObjectNode root) {
        JsonNode tc = root.get("tool_choice");
        if (tc == null || tc.isNull()) {
            return;
        }
        if (tc.isTextual()) {
            return;
        }
        if (!tc.isObject()) {
            root.remove("tool_choice");
            return;
        }
        ObjectNode choice = (ObjectNode) tc;
        String anthropicType = choice.path("type").asText("");
        switch (anthropicType) {
            case "auto" -> root.put("tool_choice", "auto");
            case "none" -> root.put("tool_choice", "none");
            case "any", "required" -> root.put("tool_choice", "required");
            case "tool" -> {
                JsonNode nameNode = choice.get("name");
                String toolName = nameNode != null && nameNode.isTextual() ? nameNode.asText() : "";
                if (!toolName.isBlank()) {
                    ObjectNode openAi = root.objectNode();
                    openAi.put("type", "function");
                    ObjectNode fn = root.objectNode();
                    fn.put("name", toolName);
                    openAi.set("function", fn);
                    root.set("tool_choice", openAi);
                } else {
                    root.put("tool_choice", "auto");
                }
            }
            default -> root.put("tool_choice", "auto");
        }
    }

    /**
     * Z.ai GLM 5 在 Bedrock Runtime 中要求自定义工具使用 custom 包装字段。
     * 输入的 Anthropic 风格工具字段会被移动到 {@code custom} 对象内。
     */
    private static ObjectNode toZaiCustomTool(ObjectNode tool) {
        ObjectNode out = tool.objectNode();
        ObjectNode custom = tool.objectNode();
        copyIfPresent(tool, custom, "name");
        copyIfPresent(tool, custom, "description");
        copyIfPresent(tool, custom, "input_schema");
        ensureToolName(custom);
        ensureInputSchema(custom);
        normalizeJsonSchemaCustomTypesToObject(custom.get("input_schema"));
        out.put("type", "custom");
        out.set("custom", custom);
        return out;
    }

    /** 将普通 Anthropic 工具里的 input_schema 递归改成 Bedrock 可接受的标准 JSON Schema。 */
    private static void normalizeToolInputSchema(ObjectNode tool) {
        JsonNode inputSchema = tool.get("input_schema");
        if (inputSchema != null) {
            normalizeJsonSchemaCustomTypesToObject(inputSchema);
        }
    }

    /** 递归地将 JSON Schema 中的 {@code type:"custom"} 改为标准的 {@code type:"object"}。 */
    private static void normalizeJsonSchemaCustomTypesToObject(JsonNode node) {
        if (node == null) {
            return;
        }
        if (node.isObject()) {
            ObjectNode obj = (ObjectNode) node;
            JsonNode type = obj.get("type");
            if (type != null && type.isTextual() && "custom".equals(type.asText())) {
                obj.put("type", "object");
            }
            obj.fields().forEachRemaining(entry -> normalizeJsonSchemaCustomTypesToObject(entry.getValue()));
        } else if (node.isArray()) {
            node.forEach(BedrockInvokeBodyPreparer::normalizeJsonSchemaCustomTypesToObject);
        }
    }

    /** 字段存在时复制，避免向 custom 对象写 null。 */
    private static void copyIfPresent(ObjectNode from, ObjectNode to, String field) {
        JsonNode value = from.get(field);
        if (value != null && !value.isNull()) {
            to.set(field, value.deepCopy());
        }
    }

    /** Z.ai Bedrock 工具 schema 要求 name，缺失时用稳定占位名兜底。 */
    private static void ensureToolName(ObjectNode custom) {
        JsonNode name = custom.get("name");
        if (name == null || name.isNull() || (name.isTextual() && name.asText().isBlank())) {
            custom.put("name", "gateway_tool");
        }
    }

    /** Z.ai Bedrock 工具 schema 要求 input_schema，缺失时补空对象 schema。 */
    private static void ensureInputSchema(ObjectNode custom) {
        JsonNode schema = custom.get("input_schema");
        if (schema == null || schema.isNull() || !schema.isObject()) {
            ObjectNode empty = custom.objectNode();
            empty.put("type", "object");
            empty.set("properties", custom.objectNode());
            custom.set("input_schema", empty);
        }
    }

    /** 目前配置中的 Z.ai 模型 ID 以 zai. 开头，按该前缀启用 GLM 5 工具包装格式。 */
    private static boolean isZaiModel(String bedrockModelId) {
        return bedrockModelId != null && bedrockModelId.startsWith("zai.");
    }

    /** Z.ai 与 DeepSeek Bedrock Runtime 请求均更接近 OpenAI ChatCompletions 结构。 */
    private static boolean usesOpenAiChatShape(String bedrockModelId) {
        return bedrockModelId != null
                && (bedrockModelId.startsWith("zai.") || bedrockModelId.startsWith("deepseek."));
    }

    /**
     * 将用户名压缩为 Bedrock metadata 可接受的 user_id：仅保留字母数字与 {@code ._:-}，超长截断。
     * 若清洗后无有效字符（例如全中文用户名），回退为固定占位符。
     */
    public static String sanitizeUserId(String username) {
        if (username == null || username.isBlank()) {
            return "gateway_user";
        }
        String s = username.replaceAll("[^a-zA-Z0-9._:-]", "_");
        boolean hasAlnum = s.chars().anyMatch(Character::isLetterOrDigit);
        if (!hasAlnum) {
            return "gateway_user";
        }
        if (s.length() > MAX_USER_ID_LEN) {
            return s.substring(0, MAX_USER_ID_LEN);
        }
        return s;
    }
}
