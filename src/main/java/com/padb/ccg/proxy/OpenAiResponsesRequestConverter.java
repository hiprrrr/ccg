package com.padb.ccg.proxy;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * 将 OpenAI Responses API（{@code POST /v1/responses}）请求体转换为 Anthropic Messages 格式，
 * 供上游 Bedrock / 华为云 MaaS 消费。
 */
public final class OpenAiResponsesRequestConverter {

    private OpenAiResponsesRequestConverter() {
    }

    /**
     * 将 Responses API JSON 请求体转换为 Anthropic Messages JSON。
     *
     * @param objectMapper Jackson 实例
     * @param responsesBody 原始 Responses API 请求体
     * @return Anthropic 格式 JSON 字符串；解析失败时返回 {@code null}
     */
    public static String toAnthropic(ObjectMapper objectMapper, String responsesBody) {
        try {
            JsonNode root = objectMapper.readTree(responsesBody);
            ObjectNode anthropic = objectMapper.createObjectNode();

            if (root.has("model")) {
                anthropic.put("model", root.get("model").asText());
            }

            StringBuilder systemText = new StringBuilder();
            if (root.has("instructions") && !root.get("instructions").isNull()) {
                appendSystemText(systemText, root.get("instructions"));
            }

            ArrayNode messages = anthropic.putArray("messages");
            JsonNode input = root.get("input");
            if (input != null && !input.isNull()) {
                if (input.isTextual()) {
                    ObjectNode userMsg = messages.addObject();
                    userMsg.put("role", "user");
                    userMsg.put("content", input.asText());
                } else if (input.isArray()) {
                    for (JsonNode item : input) {
                        convertInputItem(objectMapper, messages, systemText, item);
                    }
                }
            }

            if (systemText.length() > 0) {
                anthropic.put("system", systemText.toString());
            }

            if (root.has("max_output_tokens") && !root.get("max_output_tokens").isNull()) {
                anthropic.put("max_tokens", root.get("max_output_tokens").asInt());
            } else if (root.has("max_tokens") && !root.get("max_tokens").isNull()) {
                anthropic.put("max_tokens", root.get("max_tokens").asInt());
            } else {
                anthropic.put("max_tokens", 4096);
            }

            if (root.has("temperature") && !root.get("temperature").isNull()) {
                anthropic.put("temperature", root.get("temperature").asDouble());
            }
            if (root.has("top_p") && !root.get("top_p").isNull()) {
                anthropic.put("top_p", root.get("top_p").asDouble());
            }
            if (root.has("stop") && !root.get("stop").isNull()) {
                anthropic.set("stop_sequences", root.get("stop"));
            }
            if (root.has("stream")) {
                anthropic.put("stream", root.get("stream").asBoolean());
            }

            if (root.has("tools") && root.get("tools").isArray()) {
                ArrayNode tools = anthropic.putArray("tools");
                for (JsonNode tool : root.get("tools")) {
                    ObjectNode converted = convertTool(objectMapper, tool);
                    if (converted != null) {
                        tools.add(converted);
                    }
                }
            }

            return objectMapper.writeValueAsString(anthropic);
        } catch (Exception e) {
            return null;
        }
    }

    /**
     * 将 Responses API {@code input} 数组中的单条记录转为 Anthropic message 或并入 system。
     */
    private static void convertInputItem(ObjectMapper objectMapper, ArrayNode messages,
                                         StringBuilder systemText, JsonNode item) {
        String type = item.path("type").asText("");

        if ("message".equals(type) || item.has("role")) {
            String role = item.path("role").asText("user");
            if ("system".equals(role) || "developer".equals(role)) {
                appendSystemText(systemText, item.get("content"));
                return;
            }
            if ("assistant".equals(role)) {
                ObjectNode msg = messages.addObject();
                msg.put("role", "assistant");
                msg.set("content", toAnthropicAssistantContent(objectMapper, item.get("content")));
                return;
            }
            ObjectNode msg = messages.addObject();
            msg.put("role", "user");
            msg.set("content", toAnthropicUserContent(objectMapper, item.get("content")));
            return;
        }

        if ("function_call_output".equals(type)) {
            ObjectNode msg = messages.addObject();
            msg.put("role", "user");
            ArrayNode content = msg.putArray("content");
            ObjectNode toolResult = content.addObject();
            toolResult.put("type", "tool_result");
            toolResult.put("tool_use_id", item.path("call_id").asText(item.path("id").asText("")));
            toolResult.put("content", stringifyOutput(item.get("output")));
            return;
        }

        if ("function_call".equals(type)) {
            ObjectNode msg = messages.addObject();
            msg.put("role", "assistant");
            ArrayNode content = msg.putArray("content");
            ObjectNode toolUse = content.addObject();
            toolUse.put("type", "tool_use");
            toolUse.put("id", item.path("call_id").asText(item.path("id").asText("")));
            toolUse.put("name", item.path("name").asText(""));
            JsonNode args = item.get("arguments");
            if (args != null && args.isObject()) {
                toolUse.set("input", args);
            } else {
                String argsText = item.path("arguments").asText("{}");
                toolUse.set("input", parseJsonObjectNode(objectMapper, argsText));
            }
        }
    }

    /**
     * 将 Responses API function 工具定义转为 Anthropic tool 块。
     */
    private static ObjectNode convertTool(ObjectMapper objectMapper, JsonNode tool) {
        String toolType = tool.path("type").asText();
        if (!"function".equals(toolType)) {
            return null;
        }

        ObjectNode converted = objectMapper.createObjectNode();
        JsonNode functionNode = tool.has("function") ? tool.get("function") : tool;
        converted.put("name", functionNode.path("name").asText());
        if (functionNode.has("description") && !functionNode.get("description").isNull()) {
            converted.put("description", functionNode.path("description").asText());
        }
        if (functionNode.has("parameters") && !functionNode.get("parameters").isNull()) {
            converted.set("input_schema", functionNode.get("parameters"));
        } else if (tool.has("parameters") && !tool.get("parameters").isNull()) {
            converted.set("input_schema", tool.get("parameters"));
        }
        return converted;
    }

    /**
     * 将 user 侧 content（字符串或 input_text / input_image 数组）转为 Anthropic content。
     */
    private static JsonNode toAnthropicUserContent(ObjectMapper objectMapper, JsonNode content) {
        if (content == null || content.isNull()) {
            return objectMapper.getNodeFactory().textNode("");
        }
        if (content.isTextual()) {
            return content;
        }
        if (!content.isArray()) {
            return content;
        }

        ArrayNode anthropicContent = objectMapper.createArrayNode();
        for (JsonNode block : content) {
            String blockType = block.path("type").asText("");
            if ("input_text".equals(blockType)) {
                ObjectNode textBlock = objectMapper.createObjectNode();
                textBlock.put("type", "text");
                textBlock.put("text", block.path("text").asText(""));
                anthropicContent.add(textBlock);
            } else if ("input_image".equals(blockType)) {
                anthropicContent.add(convertInputImage(objectMapper, block));
            } else if ("text".equals(blockType)) {
                anthropicContent.add(block);
            } else if ("image_url".equals(blockType)) {
                anthropicContent.add(convertOpenAiImageUrlBlock(objectMapper, block));
            }
        }
        return anthropicContent.isEmpty() ? objectMapper.getNodeFactory().textNode("") : anthropicContent;
    }

    /**
     * 将 assistant 侧 content（output_text 等）转为 Anthropic assistant content。
     */
    private static JsonNode toAnthropicAssistantContent(ObjectMapper objectMapper, JsonNode content) {
        if (content == null || content.isNull()) {
            return objectMapper.getNodeFactory().textNode("");
        }
        if (content.isTextual()) {
            return content;
        }
        if (!content.isArray()) {
            return content;
        }

        StringBuilder text = new StringBuilder();
        for (JsonNode block : content) {
            String blockType = block.path("type").asText("");
            if ("output_text".equals(blockType) || "text".equals(blockType)) {
                if (text.length() > 0) {
                    text.append('\n');
                }
                text.append(block.path("text").asText(""));
            }
        }
        return objectMapper.getNodeFactory().textNode(text.toString());
    }

    /**
     * Responses API {@code input_image} → Anthropic image 块。
     */
    private static ObjectNode convertInputImage(ObjectMapper objectMapper, JsonNode block) {
        String url = block.path("image_url").asText("");
        ObjectNode imageBlock = objectMapper.createObjectNode();
        imageBlock.put("type", "image");
        ObjectNode source = imageBlock.putObject("source");

        int commaIdx = url.indexOf(',');
        if (url.startsWith("data:") && commaIdx > 0) {
            String header = url.substring(5, commaIdx);
            int semiIdx = header.indexOf(';');
            String mediaType = semiIdx > 0 ? header.substring(0, semiIdx) : header;
            source.put("type", "base64");
            source.put("media_type", mediaType);
            source.put("data", url.substring(commaIdx + 1));
        } else if (url.startsWith("http://") || url.startsWith("https://")) {
            source.put("type", "url");
            source.put("url", url);
        } else {
            source.put("type", "url");
            source.put("url", url);
        }
        return imageBlock;
    }

    /**
     * Chat Completions 风格 {@code image_url} 块 → Anthropic image 块。
     */
    private static ObjectNode convertOpenAiImageUrlBlock(ObjectMapper objectMapper, JsonNode block) {
        String url = block.path("image_url").path("url").asText("");
        ObjectNode imageBlock = objectMapper.createObjectNode();
        imageBlock.put("type", "image");
        ObjectNode source = imageBlock.putObject("source");

        int commaIdx = url.indexOf(',');
        if (url.startsWith("data:") && commaIdx > 0) {
            String header = url.substring(5, commaIdx);
            int semiIdx = header.indexOf(';');
            String mediaType = semiIdx > 0 ? header.substring(0, semiIdx) : header;
            source.put("type", "base64");
            source.put("media_type", mediaType);
            source.put("data", url.substring(commaIdx + 1));
        } else {
            source.put("type", "url");
            source.put("url", url);
        }
        return imageBlock;
    }

    /**
     * 将 system / developer 文本追加到顶层 {@code system} 字段缓冲区。
     */
    private static void appendSystemText(StringBuilder systemText, JsonNode content) {
        if (content == null || content.isNull()) {
            return;
        }
        if (content.isTextual()) {
            if (systemText.length() > 0) {
                systemText.append("\n\n");
            }
            systemText.append(content.asText());
            return;
        }
        if (content.isArray()) {
            for (JsonNode block : content) {
                String blockType = block.path("type").asText("");
                if ("input_text".equals(blockType) || "text".equals(blockType) || blockType.isEmpty()) {
                    if (systemText.length() > 0) {
                        systemText.append("\n\n");
                    }
                    systemText.append(block.path("text").asText(block.asText("")));
                }
            }
        }
    }

    /**
     * 将 function_call_output.output 统一序列化为字符串。
     */
    private static String stringifyOutput(JsonNode output) {
        if (output == null || output.isNull()) {
            return "";
        }
        if (output.isTextual()) {
            return output.asText();
        }
        return output.toString();
    }

    /**
     * 将 JSON 字符串解析为对象节点；非法 JSON 时返回空对象。
     */
    private static JsonNode parseJsonObjectNode(ObjectMapper objectMapper, String json) {
        String trimmed = json == null ? "" : json.trim();
        if (trimmed.isEmpty()) {
            return objectMapper.createObjectNode();
        }
        try {
            JsonNode node = objectMapper.readTree(trimmed);
            return node.isObject() ? node : objectMapper.createObjectNode();
        } catch (Exception e) {
            return objectMapper.createObjectNode();
        }
    }

    /**
     * 将 JSON 字符串解析为 JsonNode；非法 JSON 时返回空对象节点文本。
     */
    private static String parseJsonObject(String json) {
        String trimmed = json == null ? "" : json.trim();
        return trimmed.isEmpty() ? "{}" : trimmed;
    }
}
