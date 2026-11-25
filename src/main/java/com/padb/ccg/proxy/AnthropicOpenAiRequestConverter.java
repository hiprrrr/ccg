package com.padb.ccg.proxy;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.List;

/**
 * Anthropic Messages 请求体与 OpenAI Chat Completions 请求体互转（供华为 MaaS OpenAI 上游使用）。
 */
public final class AnthropicOpenAiRequestConverter {

    private AnthropicOpenAiRequestConverter() {
    }

    /**
     * 将 Anthropic {@code /v1/messages} 请求体转为 OpenAI {@code /chat/completions} 请求体。
     */
    public static String toOpenAiChat(ObjectMapper mapper, String anthropicBody,
                                      String upstreamModelId, boolean streaming)
            throws JsonProcessingException {
        JsonNode root = mapper.readTree(anthropicBody);
        if (!root.isObject()) {
            throw new JsonProcessingException("Request body must be a JSON object") {};
        }

        ObjectNode openAi = mapper.createObjectNode();
        openAi.put("model", upstreamModelId);
        openAi.put("stream", streaming);

        ArrayNode messages = openAi.putArray("messages");

        if (root.has("system") && !root.get("system").isNull()) {
            String systemText = contentToText(root.get("system"));
            if (!systemText.isBlank()) {
                ObjectNode systemMsg = messages.addObject();
                systemMsg.put("role", "system");
                systemMsg.put("content", systemText);
            }
        }

        JsonNode anthropicMessages = root.get("messages");
        if (anthropicMessages != null && anthropicMessages.isArray()) {
            for (JsonNode msg : anthropicMessages) {
                appendAnthropicMessage(mapper, messages, msg);
            }
        }

        copyIfPresent(root, openAi, "max_tokens");
        copyIfPresent(root, openAi, "temperature");
        copyIfPresent(root, openAi, "top_p");

        if (root.has("stop_sequences")) {
            openAi.set("stop", root.get("stop_sequences"));
        }

        if (root.has("tools") && root.get("tools").isArray()) {
            ArrayNode tools = openAi.putArray("tools");
            for (JsonNode tool : root.get("tools")) {
                ObjectNode fnTool = tools.addObject();
                fnTool.put("type", "function");
                ObjectNode fn = fnTool.putObject("function");
                fn.put("name", tool.path("name").asText());
                if (tool.has("description")) {
                    fn.put("description", tool.path("description").asText());
                }
                if (tool.has("input_schema")) {
                    fn.set("parameters", tool.get("input_schema"));
                }
            }
        }

        if (root.has("tool_choice") && !root.get("tool_choice").isNull()) {
            openAi.set("tool_choice", convertToolChoice(mapper, root.get("tool_choice")));
        }

        sanitizeOpenAiMessages(messages);

        return mapper.writeValueAsString(openAi);
    }

    /**
     * 将 OpenAI Chat Completions 非流式响应转为 Anthropic Message JSON（供包装为 SSE）。
     */
    public static String toAnthropicMessage(ObjectMapper mapper, String openAiResponse, String userModel)
            throws JsonProcessingException {
        JsonNode root = mapper.readTree(openAiResponse);
        ObjectNode anthropic = mapper.createObjectNode();
        anthropic.put("id", root.path("id").asText("msg_unknown"));
        anthropic.put("type", "message");
        anthropic.put("role", "assistant");
        anthropic.put("model", userModel);

        ArrayNode content = anthropic.putArray("content");
        JsonNode message = root.at("/choices/0/message");
        String text = message.path("content").asText("");
        if (!text.isEmpty()) {
            ObjectNode textBlock = content.addObject();
            textBlock.put("type", "text");
            textBlock.put("text", text);
        }

        JsonNode toolCalls = message.get("tool_calls");
        if (toolCalls != null && toolCalls.isArray()) {
            for (JsonNode call : toolCalls) {
                ObjectNode toolUse = content.addObject();
                toolUse.put("type", "tool_use");
                toolUse.put("id", call.path("id").asText());
                toolUse.put("name", call.at("/function/name").asText());
                JsonNode args = call.at("/function/arguments");
                if (args.isTextual()) {
                    toolUse.set("input", mapper.readTree(args.asText()));
                } else if (args.isObject()) {
                    toolUse.set("input", args);
                } else {
                    toolUse.putObject("input");
                }
            }
        }

        String finish = root.at("/choices/0/finish_reason").asText("stop");
        anthropic.put("stop_reason", mapFinishReason(finish));

        ObjectNode usage = anthropic.putObject("usage");
        JsonNode openAiUsage = root.path("usage");
        usage.put("input_tokens", openAiUsage.path("prompt_tokens").asInt(0));
        usage.put("output_tokens", openAiUsage.path("completion_tokens").asInt(0));

        return mapper.writeValueAsString(anthropic);
    }

    /** 将单条 Anthropic message 追加为一条或多条 OpenAI message。 */
    private static void appendAnthropicMessage(ObjectMapper mapper, ArrayNode messages, JsonNode msg) {
        String role = msg.path("role").asText("user");
        JsonNode content = msg.get("content");

        if ("user".equals(role) && content != null && content.isArray() && hasBlockType(content, "tool_result")) {
            appendUserAndToolResultMessages(mapper, messages, content);
            return;
        }
        if ("assistant".equals(role) && content != null && content.isArray() && hasBlockType(content, "tool_use")) {
            appendAssistantToolUseMessage(mapper, messages, content);
            return;
        }

        ObjectNode converted = messages.addObject();
        converted.put("role", role);
        if (content == null || content.isNull()) {
            if (!"assistant".equals(role)) {
                return;
            }
            converted.putNull("content");
        } else if (content.isTextual()) {
            String text = content.asText();
            if (text.isBlank() && !"assistant".equals(role)) {
                return;
            }
            converted.put("content", text);
        } else if (content.isArray()) {
            if (hasBlockType(content, "image")) {
                ArrayNode blocks = converted.putArray("content");
                for (JsonNode block : content) {
                    String type = block.path("type").asText();
                    if ("text".equals(type) || "image".equals(type)) {
                        blocks.add(convertAnthropicContentBlock(mapper, block));
                    }
                }
            } else {
                String text = contentToText(content);
                if (text.isBlank()) {
                    return;
                }
                converted.put("content", text);
            }
        } else {
            converted.set("content", content);
        }
    }

    /** 将含 tool_result 的 Anthropic user message 拆为 OpenAI user + tool 消息。 */
    private static void appendUserAndToolResultMessages(ObjectMapper mapper, ArrayNode messages, JsonNode content) {
        StringBuilder userText = new StringBuilder();
        ArrayNode imageBlocks = mapper.createArrayNode();
        for (JsonNode block : content) {
            if (!block.isObject()) {
                continue;
            }
            String type = block.path("type").asText();
            if ("text".equals(type)) {
                appendText(userText, block.path("text").asText());
            } else if ("image".equals(type)) {
                imageBlocks.add(convertAnthropicContentBlock(mapper, block));
            } else if ("tool_result".equals(type)) {
                String toolCallId = resolveToolCallId(block);
                String toolContent = toolResultContentText(mapper, block.get("content"));
                if (toolCallId.isBlank()) {
                    // 无法关联 assistant.tool_calls 时降级为 user 文本，避免产出空 tool_call_id 被 OpenAI 上游拒绝
                    appendText(userText, toolContent);
                    collectToolResultImages(mapper, block.get("content"), imageBlocks);
                    continue;
                }
                if (!userText.isEmpty()) {
                    ObjectNode userMsg = messages.addObject();
                    userMsg.put("role", "user");
                    userMsg.put("content", userText.toString());
                    userText.setLength(0);
                }
                ObjectNode toolMsg = messages.addObject();
                toolMsg.put("role", "tool");
                toolMsg.put("tool_call_id", toolCallId);
                toolMsg.put("content", toolContent.isBlank() ? " " : toolContent);
                collectToolResultImages(mapper, block.get("content"), imageBlocks);
            }
        }
        if (!userText.isEmpty() || !imageBlocks.isEmpty()) {
            ObjectNode userMsg = messages.addObject();
            userMsg.put("role", "user");
            if (imageBlocks.isEmpty()) {
                userMsg.put("content", userText.toString());
            } else {
                ArrayNode blocks = userMsg.putArray("content");
                if (!userText.isEmpty()) {
                    ObjectNode textBlock = blocks.addObject();
                    textBlock.put("type", "text");
                    textBlock.put("text", userText.toString());
                }
                blocks.addAll(imageBlocks);
            }
        }
    }

    /**
     * 收集 tool_result.content 中的 image 块并转为 OpenAI image_url 块。
     * OpenAI tool 角色消息只接受字符串 content，无法携带图片，
     * 需由随后的 user 消息承载，否则视觉上游会丢失工具结果中的图片。
     */
    private static void collectToolResultImages(ObjectMapper mapper, JsonNode toolResultContent, ArrayNode imageBlocks) {
        JsonNode content = toolResultContent;
        if (content != null && content.isTextual()) {
            // 部分客户端会把 content 块数组整体序列化为字符串
            String text = content.asText();
            if (!text.contains("\"image\"") && !text.contains("\"image_url\"")) {
                return;
            }
            try {
                content = mapper.readTree(text);
            } catch (JsonProcessingException e) {
                return;
            }
        }
        if (content == null || !content.isArray()) {
            return;
        }
        for (JsonNode block : content) {
            if (block.isObject() && "image".equals(block.path("type").asText())) {
                imageBlocks.add(convertAnthropicContentBlock(mapper, block));
            }
        }
    }

    /**
     * 从 Anthropic {@code tool_result} 块解析 OpenAI {@code tool_call_id}。
     * 依次尝试 {@code tool_use_id}、{@code tool_call_id}、{@code id}，兼容不同客户端字段命名；
     * 与 Bedrock 转换路径的解析规则保持一致。
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

    /**
     * tool_result.content 的文本化：字符串化的块数组解析成功后只取 text 块。
     * 直接把原始 JSON 串当 tool 消息文本会让 base64 图片数据同时出现在
     * tool 消息与后续 user 消息的 image_url 块里，导致 prompt 体积翻倍。
     */
    private static String toolResultContentText(ObjectMapper mapper, JsonNode content) {
        if (content != null && content.isTextual()) {
            try {
                JsonNode parsed = mapper.readTree(content.asText());
                if (parsed.isArray()) {
                    return contentToText(parsed);
                }
            } catch (JsonProcessingException ignored) {
                // 非 JSON 字符串按原文处理
            }
        }
        return contentToText(content);
    }

    /** 将含 tool_use 的 Anthropic assistant message 转为 OpenAI assistant.tool_calls。 */
    private static void appendAssistantToolUseMessage(ObjectMapper mapper, ArrayNode messages, JsonNode content) {
        ObjectNode assistantMsg = messages.addObject();
        assistantMsg.put("role", "assistant");
        StringBuilder text = new StringBuilder();
        ArrayNode toolCalls = mapper.createArrayNode();
        for (JsonNode block : content) {
            if (!block.isObject()) {
                continue;
            }
            String type = block.path("type").asText();
            if ("text".equals(type)) {
                appendText(text, block.path("text").asText());
            } else if ("tool_use".equals(type)) {
                ObjectNode call = mapper.createObjectNode();
                call.put("id", block.path("id").asText());
                call.put("type", "function");
                ObjectNode function = mapper.createObjectNode();
                function.put("name", block.path("name").asText());
                JsonNode input = block.get("input");
                function.put("arguments", input != null && !input.isNull() ? input.toString() : "{}");
                call.set("function", function);
                toolCalls.add(call);
            }
        }
        if (!toolCalls.isEmpty()) {
            assistantMsg.set("tool_calls", toolCalls);
            if (text.isEmpty()) {
                assistantMsg.remove("content");
            } else {
                assistantMsg.put("content", text.toString());
            }
        } else {
            assistantMsg.put("content", text.toString());
        }
    }

    private static JsonNode convertToolChoice(ObjectMapper mapper, JsonNode toolChoice) {
        if (toolChoice.isTextual()) {
            String value = toolChoice.asText();
            if ("auto".equals(value) || "any".equals(value) || "none".equals(value)) {
                return toolChoice;
            }
            if ("required".equals(value)) {
                return mapper.getNodeFactory().textNode("required");
            }
        }
        if (toolChoice.isObject()) {
            String type = toolChoice.path("type").asText("");
            if ("tool".equals(type) && toolChoice.has("name")) {
                ObjectNode fn = mapper.createObjectNode();
                fn.put("type", "function");
                ObjectNode function = fn.putObject("function");
                function.put("name", toolChoice.path("name").asText());
                return fn;
            }
        }
        return toolChoice;
    }

    private static JsonNode convertAnthropicContentBlock(ObjectMapper mapper, JsonNode block) {
        String type = block.path("type").asText();
        if ("text".equals(type)) {
            ObjectNode text = mapper.createObjectNode();
            text.put("type", "text");
            text.put("text", block.path("text").asText(""));
            return text;
        }
        if ("image".equals(type)) {
            JsonNode source = block.path("source");
            String sourceType = source.path("type").asText();
            ObjectNode imageUrl = mapper.createObjectNode();
            imageUrl.put("type", "image_url");
            ObjectNode urlNode = imageUrl.putObject("image_url");
            if ("base64".equals(sourceType)) {
                String mediaType = source.path("media_type").asText("image/png");
                String data = source.path("data").asText("");
                urlNode.put("url", "data:" + mediaType + ";base64," + data);
            } else if ("url".equals(sourceType)) {
                urlNode.put("url", source.path("url").asText(""));
            } else {
                urlNode.put("url", "");
            }
            return imageUrl;
        }
        return block;
    }

    private static boolean hasBlockType(JsonNode content, String type) {
        if (content == null || !content.isArray()) {
            return false;
        }
        for (JsonNode block : content) {
            if (type.equals(block.path("type").asText())) {
                return true;
            }
        }
        return false;
    }

    private static String contentToText(JsonNode content) {
        if (content == null || content.isNull()) {
            return "";
        }
        if (content.isTextual() || content.isNumber() || content.isBoolean()) {
            return content.asText();
        }
        if (content.isArray()) {
            StringBuilder sb = new StringBuilder();
            for (JsonNode block : content) {
                if (block.isTextual()) {
                    appendText(sb, block.asText());
                } else if (block.isObject() && "text".equals(block.path("type").asText())) {
                    appendText(sb, block.path("text").asText());
                }
            }
            return sb.toString();
        }
        return content.asText("");
    }

    private static void appendText(StringBuilder sb, String text) {
        if (text == null || text.isEmpty()) {
            return;
        }
        if (!sb.isEmpty()) {
            sb.append('\n');
        }
        sb.append(text);
    }

    /**
     * 清理 OpenAI messages：去掉空 user/system，assistant 仅有 tool_calls 时移除空 content，
     * 避免华为 MaaS Jinja 模板渲染出空 prompt。
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
                if (isBlankContent(obj.get("content"))) {
                    obj.put("content", " ");
                }
            }
        }
    }

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

    private static void copyIfPresent(JsonNode from, ObjectNode to, String field) {
        if (from.has(field) && !from.get(field).isNull()) {
            to.set(field, from.get(field));
        }
    }

    private static String mapFinishReason(String openAiFinish) {
        if (openAiFinish == null || openAiFinish.isBlank()) {
            return "end_turn";
        }
        return switch (openAiFinish) {
            case "length" -> "max_tokens";
            case "tool_calls", "function_call" -> "tool_use";
            default -> "end_turn";
        };
    }
}
