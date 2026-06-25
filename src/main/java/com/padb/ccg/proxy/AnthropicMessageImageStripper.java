package com.padb.ccg.proxy;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * 从 Anthropic Messages 请求体中移除图片类 content 块（{@code image} / {@code image_url}），
 * 替换为占位文本，供不支持视觉能力的上游使用。
 */
public final class AnthropicMessageImageStripper {

    /** 默认占位文案：说明图片已被省略，避免模型误以为仍有视觉输入 */
    public static final String DEFAULT_PLACEHOLDER = "[image omitted: model does not support vision]";

    private AnthropicMessageImageStripper() {
    }

    /**
     * 遍历 {@code messages} 及其嵌套 content（含 tool_result），将图片块替换为 text 占位块。
     * 不含图片时原样返回，避免大请求体无谓序列化。
     */
    public static String stripImageBlocks(ObjectMapper mapper, String requestBody, String placeholderText)
            throws JsonProcessingException {
        if (requestBody == null
                || (!requestBody.contains("\"image\"") && !requestBody.contains("\"image_url\""))) {
            return requestBody;
        }
        JsonNode rootNode = mapper.readTree(requestBody);
        if (!(rootNode instanceof ObjectNode root)) {
            return requestBody;
        }
        JsonNode messages = root.path("messages");
        if (!messages.isArray()) {
            return requestBody;
        }
        boolean changed = false;
        for (JsonNode message : messages) {
            if (!(message instanceof ObjectNode msgObj)) {
                continue;
            }
            JsonNode content = msgObj.get("content");
            if (content instanceof ArrayNode contentArr) {
                changed |= sanitizeContentArray(mapper, contentArr, placeholderText);
            }
        }
        return changed ? mapper.writeValueAsString(root) : requestBody;
    }

    /**
     * 就地处理 content 数组：图片块 → text 占位；tool_result 内嵌 content 递归处理。
     */
    private static boolean sanitizeContentArray(ObjectMapper mapper, ArrayNode contentArr, String placeholderText) {
        boolean changed = false;
        for (int i = 0; i < contentArr.size(); i++) {
            JsonNode block = contentArr.get(i);
            if (!block.isObject()) {
                continue;
            }
            String type = block.path("type").asText();
            if ("image".equals(type) || "image_url".equals(type)) {
                contentArr.set(i, textPlaceholder(mapper, placeholderText));
                changed = true;
            } else if ("tool_result".equals(type) && block instanceof ObjectNode toolResult) {
                changed |= sanitizeToolResultContent(mapper, toolResult, placeholderText);
            }
        }
        return changed;
    }

    /** tool_result.content 可能是字符串或 content 块数组，均需检查是否含图片 */
    private static boolean sanitizeToolResultContent(ObjectMapper mapper, ObjectNode toolResult, String placeholderText) {
        JsonNode inner = toolResult.get("content");
        if (inner instanceof ArrayNode innerArr) {
            return sanitizeContentArray(mapper, innerArr, placeholderText);
        }
        if (inner != null && inner.isTextual()
                && (inner.asText().contains("\"image\"") || inner.asText().contains("\"image_url\""))) {
            try {
                JsonNode parsed = mapper.readTree(inner.asText());
                if (parsed instanceof ArrayNode parsedArr) {
                    if (sanitizeContentArray(mapper, parsedArr, placeholderText)) {
                        toolResult.put("content", mapper.writeValueAsString(parsedArr));
                        return true;
                    }
                }
            } catch (JsonProcessingException ignored) {
                // 非 JSON 字符串时跳过
            }
        }
        return false;
    }

    private static ObjectNode textPlaceholder(ObjectMapper mapper, String text) {
        ObjectNode block = mapper.createObjectNode();
        block.put("type", "text");
        block.put("text", text);
        return block;
    }
}
