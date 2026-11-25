package com.padb.ccg.proxy;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.List;
import java.util.Locale;

/**
 * 将 Anthropic Messages 请求体转换为 Amazon Nova 原生 schema。
 *
 * <p>Nova 的请求体与 Converse 同构（依据 AWS Nova User Guide 的 request schema）：</p>
 * <pre>
 * {
 *   "system": [{"text": "..."}],
 *   "messages": [
 *     {"role": "user", "content": [{"text": "..."},
 *                                  {"image": {"format": "png", "source": {"bytes": "&lt;base64&gt;"}}}]},
 *     {"role": "assistant", "content": [{"toolUse": {"toolUseId": "...", "name": "...", "input": {}}}]},
 *     {"role": "user", "content": [{"toolResult": {"toolUseId": "...", "content": [{"text": "..."}],
 *                                          "status": "success"}}]}
 *   ],
 *   "inferenceConfig": {"maxTokens": 1024, "temperature": 0.7, "topP": 0.9, "stopSequences": ["..."]},
 *   "toolConfig": {"tools": [{"toolSpec": {"name": "...", "description": "...",
 *                                        "inputSchema": {"json": {}}}}],
 *                  "toolChoice": {"auto": {}}}
 * }
 * </pre>
 *
 * <p>Nova 不支持 URL 形式的图片源，也不支持 thinking 块，这两类内容会被替换为占位文本，
 * 避免上游因未知结构报错。</p>
 */
public final class BedrockNovaBodyPreparer {

    /** Nova 支持的图片格式（其余格式按 png 处理）。 */
    private static final List<String> SUPPORTED_IMAGE_FORMATS = List.of("png", "jpeg", "gif", "webp");

    /** 图片为 URL 时的占位文案。 */
    private static final String URL_IMAGE_PLACEHOLDER = "[image omitted: Nova only accepts base64 image bytes]";

    /** thinking 块被丢弃时的占位文案。 */
    private static final String THINKING_PLACEHOLDER = "[thinking omitted: Nova does not support thinking blocks]";

    private BedrockNovaBodyPreparer() {
    }

    /**
     * 将 Anthropic 请求体转换为 Nova 请求体。
     *
     * @param mapper       Jackson 映射器
     * @param anthropicBody Anthropic Messages 请求体 JSON
     * @return Nova 请求体 JSON
     */
    public static String toNovaBody(ObjectMapper mapper, String anthropicBody) throws JsonProcessingException {
        JsonNode root = mapper.readTree(anthropicBody);
        if (!root.isObject()) {
            return anthropicBody;
        }
        ObjectNode nova = mapper.createObjectNode();

        // system：Anthropic 的字符串或块数组 → Nova 的 [{text}]
        ArrayNode system = nova.putArray("system");
        appendSystem(mapper, system, root.get("system"));

        // messages：Anthropic messages → Nova messages
        ArrayNode messages = nova.putArray("messages");
        JsonNode anthropicMessages = root.get("messages");
        if (anthropicMessages != null && anthropicMessages.isArray()) {
            for (JsonNode message : anthropicMessages) {
                appendMessage(mapper, messages, message);
            }
        }

        // inferenceConfig：max_tokens / temperature / top_p / stop_sequences
        ObjectNode inferenceConfig = nova.putObject("inferenceConfig");
        inferenceConfig.put("maxTokens", root.path("max_tokens").asInt(1024));
        if (root.hasNonNull("temperature")) {
            inferenceConfig.put("temperature", root.path("temperature").asDouble());
        }
        if (root.hasNonNull("top_p")) {
            inferenceConfig.put("topP", root.path("top_p").asDouble());
        }
        JsonNode stopSequences = root.get("stop_sequences");
        if (stopSequences != null && stopSequences.isArray() && !stopSequences.isEmpty()) {
            ArrayNode stops = inferenceConfig.putArray("stopSequences");
            stopSequences.forEach(s -> stops.add(s.asText()));
        }

        // toolConfig：tools + tool_choice
        appendToolConfig(mapper, nova, root);
        return mapper.writeValueAsString(nova);
    }

    /** Anthropic system（字符串或块数组）→ Nova system 块数组。 */
    private static void appendSystem(ObjectMapper mapper, ArrayNode system, JsonNode anthropicSystem) {
        if (anthropicSystem == null || anthropicSystem.isNull()) {
            return;
        }
        if (anthropicSystem.isTextual()) {
            String text = anthropicSystem.asText();
            if (!text.isBlank()) {
                ObjectNode block = system.addObject();
                block.put("text", text);
            }
            return;
        }
        if (!anthropicSystem.isArray()) {
            return;
        }
        for (JsonNode block : anthropicSystem) {
            if (!block.isObject()) {
                continue;
            }
            String type = block.path("type").asText("text");
            if (!"text".equals(type)) {
                continue;
            }
            String text = block.path("text").asText("");
            if (text.isBlank()) {
                continue;
            }
            ObjectNode novaBlock = system.addObject();
            novaBlock.put("text", text);
        }
    }

    /** 单条 Anthropic message → Nova message。 */
    private static void appendMessage(ObjectMapper mapper, ArrayNode messages, JsonNode message) {
        if (!message.isObject()) {
            return;
        }
        String role = message.path("role").asText("user");
        JsonNode content = message.get("content");
        ObjectNode novaMessage = mapper.createObjectNode();
        novaMessage.put("role", role);
        ArrayNode novaContent = novaMessage.putArray("content");

        if (content == null || content.isNull()) {
            messages.add(novaMessage);
            return;
        }
        if (content.isTextual()) {
            ObjectNode textBlock = novaContent.addObject();
            textBlock.put("text", content.asText());
            messages.add(novaMessage);
            return;
        }
        if (!content.isArray()) {
            messages.add(novaMessage);
            return;
        }
        for (JsonNode block : content) {
            appendContentBlock(mapper, novaContent, block);
        }
        messages.add(novaMessage);
    }

    /** 单个 Anthropic content block → Nova content block。 */
    private static void appendContentBlock(ObjectMapper mapper, ArrayNode novaContent, JsonNode block) {
        if (!block.isObject()) {
            return;
        }
        String type = block.path("type").asText("");
        switch (type) {
            case "text" -> {
                ObjectNode textBlock = novaContent.addObject();
                textBlock.put("text", block.path("text").asText(""));
            }
            case "image" -> appendImage(mapper, novaContent, block);
            case "tool_use" -> {
                ObjectNode toolUse = novaContent.addObject().putObject("toolUse");
                toolUse.put("toolUseId", block.path("id").asText(""));
                toolUse.put("name", block.path("name").asText(""));
                JsonNode input = block.get("input");
                if (input != null && input.isObject()) {
                    toolUse.set("input", input.deepCopy());
                } else {
                    toolUse.putObject("input");
                }
            }
            case "tool_result" -> appendToolResult(mapper, novaContent, block);
            case "thinking" -> {
                ObjectNode placeholder = novaContent.addObject();
                placeholder.put("text", THINKING_PLACEHOLDER);
            }
            default -> {
                // 未知块类型忽略，避免 Nova 因未知结构报错
            }
        }
    }

    /** Anthropic image 块 → Nova image 块（仅支持 base64 字节）。 */
    private static void appendImage(ObjectMapper mapper, ArrayNode novaContent, JsonNode block) {
        JsonNode source = block.path("source");
        String sourceType = source.path("type").asText("");
        if (!"base64".equals(sourceType)) {
            ObjectNode placeholder = novaContent.addObject();
            placeholder.put("text", URL_IMAGE_PLACEHOLDER);
            return;
        }
        String mediaType = source.path("media_type").asText("image/png").toLowerCase(Locale.ROOT);
        String format = mediaType.contains("/") ? mediaType.substring(mediaType.indexOf('/') + 1) : mediaType;
        if (!SUPPORTED_IMAGE_FORMATS.contains(format)) {
            format = "png";
        }
        ObjectNode image = novaContent.addObject().putObject("image");
        image.put("format", format);
        ObjectNode bytes = image.putObject("source");
        bytes.put("bytes", source.path("data").asText(""));
    }

    /** Anthropic tool_result 块 → Nova toolResult 块。 */
    private static void appendToolResult(ObjectMapper mapper, ArrayNode novaContent, JsonNode block) {
        ObjectNode toolResult = novaContent.addObject().putObject("toolResult");
        toolResult.put("toolUseId", block.path("tool_use_id").asText(""));
        ArrayNode resultContent = toolResult.putArray("content");
        appendToolResultContent(mapper, resultContent, block.get("content"));
        toolResult.put("status", "success");
    }

    /** tool_result 的内容（字符串或块数组）→ Nova toolResult.content 文本块。 */
    private static void appendToolResultContent(ObjectMapper mapper, ArrayNode resultContent, JsonNode content) {
        if (content == null || content.isNull()) {
            return;
        }
        if (content.isTextual()) {
            ObjectNode textBlock = resultContent.addObject();
            textBlock.put("text", content.asText());
            return;
        }
        if (!content.isArray()) {
            return;
        }
        for (JsonNode block : content) {
            if (!block.isObject()) {
                continue;
            }
            String type = block.path("type").asText("");
            if ("text".equals(type)) {
                ObjectNode textBlock = resultContent.addObject();
                textBlock.put("text", block.path("text").asText(""));
            } else if ("image".equals(type)) {
                // Nova 的 toolResult 不接受图片块，用占位文本代替
                ObjectNode placeholder = resultContent.addObject();
                placeholder.put("text", URL_IMAGE_PLACEHOLDER);
            }
        }
    }

    /** Anthropic tools + tool_choice → Nova toolConfig。 */
    private static void appendToolConfig(ObjectMapper mapper, ObjectNode nova, JsonNode root) {
        JsonNode tools = root.get("tools");
        boolean hasTools = tools != null && tools.isArray() && !tools.isEmpty();

        JsonNode toolChoice = root.get("tool_choice");
        String choiceType = toolChoice != null && toolChoice.isObject()
                ? toolChoice.path("type").asText("") : "";

        // tool_choice=none 表示不暴露工具，Nova 没有对应语义，直接不下发 toolConfig
        if ("none".equals(choiceType)) {
            return;
        }
        if (!hasTools) {
            return;
        }

        ObjectNode toolConfig = nova.putObject("toolConfig");
        ArrayNode novaTools = toolConfig.putArray("tools");
        for (JsonNode tool : tools) {
            if (!tool.isObject()) {
                continue;
            }
            ObjectNode toolSpec = novaTools.addObject().putObject("toolSpec");
            toolSpec.put("name", tool.path("name").asText(""));
            String description = tool.path("description").asText("");
            if (!description.isBlank()) {
                toolSpec.put("description", description);
            }
            JsonNode inputSchema = tool.get("input_schema");
            ObjectNode schemaHolder = toolSpec.putObject("inputSchema");
            if (inputSchema != null && inputSchema.isObject()) {
                schemaHolder.set("json", inputSchema.deepCopy());
            } else {
                schemaHolder.putObject("json");
            }
        }

        ObjectNode novaChoice = toolConfig.putObject("toolChoice");
        switch (choiceType) {
            case "any", "required" -> novaChoice.putObject("any");
            case "tool" -> novaChoice.putObject("tool").put("name", toolChoice.path("name").asText(""));
            default -> novaChoice.putObject("auto");
        }
    }
}
