package com.padb.ccg.proxy;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * OpenAI Chat Completions 请求体 → Anthropic Messages 请求体转换器。
 * 转换失败（非 JSON 等）返回 null，由调用方决定回退原样透传还是报错。
 */
public final class OpenAiChatRequestConverter {

    private static final Logger log = LoggerFactory.getLogger(OpenAiChatRequestConverter.class);

    private OpenAiChatRequestConverter() {
    }

    /**
     * 转换 OpenAI 请求体为 Anthropic 格式；失败返回 null。
     */
    public static String toAnthropic(ObjectMapper objectMapper, String openAiBody) {
        try {
            JsonNode root = objectMapper.readTree(openAiBody);
            ObjectNode anthropic = objectMapper.createObjectNode();

            // 模型名称
            if (root.has("model")) {
                anthropic.put("model", root.get("model").asText());
            }

            // 消息转换：OpenAI 把 system 放在 messages 里，Anthropic 要求顶层 system 字段，
            // 因此把 role=system 的消息抽出来拼成顶层 system，其余消息原样保留
            if (root.has("messages")) {
                ArrayNode messages = anthropic.putArray("messages");
                StringBuilder systemText = new StringBuilder();
                for (JsonNode msg : root.get("messages")) {
                    String role = msg.path("role").asText();

                    // system 消息：提取文本内容到顶层 system 字段，跳过 messages 数组
                    if ("system".equals(role)) {
                        JsonNode sysContent = msg.get("content");
                        if (sysContent != null && sysContent.isTextual()) {
                            if (systemText.length() > 0) {
                                systemText.append("\n\n");
                            }
                            systemText.append(sysContent.asText());
                        }
                        continue;
                    }

                    // OpenAI role=tool → Anthropic user + tool_result，保留 tool_call_id 供 Bedrock glm-5 转换
                    if ("tool".equals(role)) {
                        appendOpenAiToolMessageAsAnthropic(messages, msg);
                        continue;
                    }

                    // OpenAI assistant.tool_calls → Anthropic assistant + tool_use 内容块
                    if ("assistant".equals(role) && hasOpenAiToolCalls(msg)) {
                        appendOpenAiAssistantToolCallsAsAnthropic(objectMapper, messages, msg);
                        continue;
                    }

                    ObjectNode convertedMsg = messages.addObject();
                    convertedMsg.put("role", role);

                    JsonNode content = msg.get("content");
                    if (content != null) {
                        if (content.isTextual()) {
                            convertedMsg.put("content", content.asText());
                        } else if (content.isArray()) {
                            // 逐块翻译 OpenAI content 块为 Anthropic 格式：
                            // - text 原样保留
                            // - image_url 拆成 Anthropic image 块（data URI → base64，http(s) → url）
                            // 其他不认识的类型原样透传，由上游校验
                            ArrayNode anthropicContent = convertedMsg.putArray("content");
                            for (JsonNode block : content) {
                                anthropicContent.add(convertContentBlock(objectMapper, block));
                            }
                        }
                    }
                }
                if (systemText.length() > 0) {
                    anthropic.put("system", systemText.toString());
                }
            }

            // max_tokens
            if (root.has("max_tokens")) {
                anthropic.put("max_tokens", root.get("max_tokens").asInt());
            } else {
                anthropic.put("max_tokens", 4096); // 默认值
            }

            // temperature
            if (root.has("temperature")) {
                anthropic.put("temperature", root.get("temperature").asDouble());
            }

            // top_p
            if (root.has("top_p")) {
                anthropic.put("top_p", root.get("top_p").asDouble());
            }

            // stop
            if (root.has("stop")) {
                anthropic.set("stop_sequences", root.get("stop"));
            }

            // stream
            if (root.has("stream")) {
                anthropic.put("stream", root.get("stream").asBoolean());
            }

            // tools (function calling)
            if (root.has("tools")) {
                ArrayNode tools = anthropic.putArray("tools");
                for (JsonNode tool : root.get("tools")) {
                    ObjectNode convertedTool = tools.addObject();
                    String toolType = tool.path("type").asText();
                    if ("function".equals(toolType)) {
                        convertedTool.put("name", tool.path("function").path("name").asText());
                        if (tool.path("function").has("description")) {
                            convertedTool.put("description", tool.path("function").path("description").asText());
                        }
                        if (tool.path("function").has("parameters")) {
                            convertedTool.set("input_schema", tool.path("function").get("parameters"));
                        }
                    }
                }
            }

            return objectMapper.writeValueAsString(anthropic);
        } catch (Exception e) {
            log.warn("Failed to convert OpenAI request to Anthropic format: {}", e.getMessage());
            return null;
        }
    }

    /** 判断 OpenAI assistant 消息是否含非空 tool_calls。 */
    private static boolean hasOpenAiToolCalls(JsonNode msg) {
        JsonNode toolCalls = msg.get("tool_calls");
        return toolCalls != null && toolCalls.isArray() && !toolCalls.isEmpty();
    }

    /**
     * OpenAI {@code role:tool} 转为 Anthropic user message + {@code tool_result} 块。
     * 缺 {@code tool_call_id} 时降级为 user 文本，避免 Bedrock 收到缺字段的 tool 消息。
     */
    private static void appendOpenAiToolMessageAsAnthropic(ArrayNode messages, JsonNode msg) {
        String toolCallId = msg.path("tool_call_id").asText("");
        JsonNode content = msg.get("content");
        if (toolCallId.isBlank()) {
            ObjectNode userMsg = messages.addObject();
            userMsg.put("role", "user");
            userMsg.put("content", openAiMessageContentToText(content));
            return;
        }
        ObjectNode userMsg = messages.addObject();
        userMsg.put("role", "user");
        ArrayNode blocks = userMsg.putArray("content");
        ObjectNode toolResult = blocks.addObject();
        toolResult.put("type", "tool_result");
        toolResult.put("tool_use_id", toolCallId);
        if (content == null || content.isNull()) {
            toolResult.put("content", " ");
        } else if (content.isTextual()) {
            String text = content.asText();
            toolResult.put("content", text.isBlank() ? " " : text);
        } else {
            toolResult.set("content", content);
        }
    }

    /**
     * OpenAI assistant {@code tool_calls} 转为 Anthropic assistant + {@code tool_use} 内容块。
     */
    private static void appendOpenAiAssistantToolCallsAsAnthropic(ObjectMapper objectMapper,
                                                                  ArrayNode messages, JsonNode msg) {
        ObjectNode assistantMsg = messages.addObject();
        assistantMsg.put("role", "assistant");
        ArrayNode anthropicContent = assistantMsg.putArray("content");

        JsonNode content = msg.get("content");
        if (content != null && content.isTextual() && !content.asText().isBlank()) {
            ObjectNode textBlock = anthropicContent.addObject();
            textBlock.put("type", "text");
            textBlock.put("text", content.asText());
        } else if (content != null && content.isArray()) {
            for (JsonNode block : content) {
                anthropicContent.add(convertContentBlock(objectMapper, block));
            }
        }

        for (JsonNode call : msg.get("tool_calls")) {
            ObjectNode toolUse = anthropicContent.addObject();
            toolUse.put("type", "tool_use");
            toolUse.put("id", call.path("id").asText());
            toolUse.put("name", call.at("/function/name").asText());
            JsonNode args = call.at("/function/arguments");
            if (args.isTextual()) {
                try {
                    toolUse.set("input", objectMapper.readTree(args.asText()));
                } catch (Exception e) {
                    toolUse.putObject("input");
                }
            } else if (args.isObject()) {
                toolUse.set("input", args);
            } else {
                toolUse.putObject("input");
            }
        }
    }

    /** 将 OpenAI message content（字符串或块数组）压缩为纯文本。 */
    private static String openAiMessageContentToText(JsonNode content) {
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
                    if (!sb.isEmpty()) {
                        sb.append('\n');
                    }
                    sb.append(block.asText());
                } else if (block.isObject() && "text".equals(block.path("type").asText())) {
                    if (!sb.isEmpty()) {
                        sb.append('\n');
                    }
                    sb.append(block.path("text").asText());
                }
            }
            return sb.toString();
        }
        return content.asText("");
    }

    /**
     * 将单个 OpenAI content 块翻译为 Anthropic content 块。
     * - text: 原样保留
     * - image_url (data URI): → image / source.type=base64，拆出 media_type 与 base64 data
     * - image_url (http/https): → image / source.type=url
     * - 其他: 原样透传，交由上游校验
     */
    private static JsonNode convertContentBlock(ObjectMapper objectMapper, JsonNode block) {
        String type = block.path("type").asText();
        if ("image_url".equals(type)) {
            String url = block.path("image_url").path("url").asText("");
            int commaIdx = url.indexOf(',');
            if (url.startsWith("data:") && commaIdx > 0) {
                // data:image/png;base64,XXXX → media_type=image/png, data=XXXX
                String header = url.substring(5, commaIdx); // 形如 image/png;base64
                int semiIdx = header.indexOf(';');
                String mediaType = semiIdx > 0 ? header.substring(0, semiIdx) : header;
                String data = url.substring(commaIdx + 1);
                ObjectNode imageBlock = objectMapper.createObjectNode();
                imageBlock.put("type", "image");
                ObjectNode source = imageBlock.putObject("source");
                source.put("type", "base64");
                source.put("media_type", mediaType);
                source.put("data", data);
                return imageBlock;
            } else if (url.startsWith("http://") || url.startsWith("https://")) {
                ObjectNode imageBlock = objectMapper.createObjectNode();
                imageBlock.put("type", "image");
                ObjectNode source = imageBlock.putObject("source");
                source.put("type", "url");
                source.put("url", url);
                return imageBlock;
            }
        }
        return block;
    }
}
