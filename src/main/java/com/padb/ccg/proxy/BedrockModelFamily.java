package com.padb.ccg.proxy;

import java.util.List;
import java.util.Locale;
import java.util.Set;

/**
 * Bedrock 上游模型的协议家族，决定网关与上游之间的请求体格式与响应格式。
 *
 * <p>Bedrock 的 Invoke API 对不同模型家族使用不同的 body schema，官方模型卡示例：</p>
 * <ul>
 *   <li>{@code anthropic.claude-*}：Anthropic Messages 原生格式</li>
 *   <li>{@code zai.*} / {@code deepseek.*} / {@code qwen.*} / {@code moonshotai.*} /
 *       {@code meta.*} / {@code google.*} / {@code nvidia.*} / {@code minimax.*} /
 *       {@code writer.*} / {@code openai.*}：OpenAI Chat Completions 格式</li>
 *   <li>{@code amazon.nova-*}：Amazon Nova 原生 schema
 *       （{@code messages[].content[].text} + {@code inferenceConfig} + {@code toolConfig}）</li>
 *   <li>{@code cohere.*}（{@code message} 单字段）、{@code ai21.*}（自有格式）、
 *       老版本 {@code meta.llama2*} / {@code mistral.*} text completion（{@code prompt} 式）：暂不支持</li>
 * </ul>
 *
 * <p>模型 ID 可能是跨区推理配置 ID（{@code us.} / {@code eu.} / {@code global.} 等前缀），
 * 判定前需先剥离 geo 前缀再取厂商段。</p>
 */
public enum BedrockModelFamily {

    /** Anthropic Messages 形态：请求体保持 Anthropic 格式，响应为 Anthropic SSE 事件。 */
    ANTHROPIC,

    /** OpenAI Chat Completions 形态：请求体转 OpenAI 格式，响应为 OpenAI chunk。 */
    OPENAI_CHAT,

    /** Amazon Nova 原生形态：请求体转 Nova schema，响应为 Nova 事件。 */
    NOVA,

    /** 暂不支持的家族，请求会在发出前被拒绝。 */
    UNSUPPORTED;

    /**
     * 跨区推理配置（inference profile）前缀：形如 {@code us.amazon.nova-pro-v1:0}、{@code global.moonshotai.kimi-k3}。
     * 这些前缀不是厂商段，判定家族时必须先剥离。
     */
    private static final Set<String> GEO_PREFIXES = Set.of(
            "us", "eu", "apac", "global", "in", "us-gov", "jp", "ca", "au", "sa", "uk", "me", "ap");

    /** 厂商段 → 家族。依据 AWS 官方模型卡的 InvokeModel body 示例。 */
    private static final Set<String> OPENAI_CHAT_VENDORS = Set.of(
            "zai", "deepseek", "qwen", "moonshotai", "meta", "google", "nvidia",
            "minimax", "writer", "openai", "mistral");

    /**
     * 按上游模型 ID 判定协议家族。
     *
     * @param modelId Bedrock 模型 ID（model-mappings 的 upstream-model-id），可为跨区推理配置 ID
     * @return 协议家族；无法识别时返回 {@link #UNSUPPORTED}
     */
    public static BedrockModelFamily of(String modelId) {
        String vendor = vendorOf(modelId);
        if (vendor == null) {
            return UNSUPPORTED;
        }
        return ofVendor(vendor, modelId.trim().toLowerCase(Locale.ROOT));
    }

    /**
     * 取模型 ID 的厂商段：先剥离跨区推理配置前缀（{@code us.} / {@code eu.} / {@code global.} 等），
     * 再取第一个点号分段。家族判定与其他按厂商区分的逻辑（如 Z.ai 工具包装）必须共用本方法，
     * 避免出现「家族按剥前缀判定、厂商特性按原始前缀判定」的错配。
     *
     * @param modelId Bedrock 模型 ID，可为跨区推理配置 ID
     * @return 小写厂商段（如 {@code amazon} / {@code zai}）；无法解析时返回 null
     */
    public static String vendorOf(String modelId) {
        if (modelId == null || modelId.isBlank()) {
            return null;
        }
        String id = modelId.trim().toLowerCase(Locale.ROOT);
        int dot = id.indexOf('.');
        if (dot <= 0) {
            return null;
        }
        String first = id.substring(0, dot);
        if (!GEO_PREFIXES.contains(first)) {
            return first;
        }
        // 跨区推理配置：us.amazon.nova-pro-v1:0 → 厂商段为 amazon
        String rest = id.substring(dot + 1);
        int dot2 = rest.indexOf('.');
        if (dot2 <= 0) {
            return null;
        }
        return rest.substring(0, dot2);
    }

    /**
     * 按显式配置判定家族；配置为空时回退到按模型 ID 推断。
     *
     * @param modelId       Bedrock 模型 ID
     * @param upstreamFormat 显式声明的上游格式（openai / anthropic / nova），可为 null
     * @return 协议家族
     */
    public static BedrockModelFamily of(String modelId, String upstreamFormat) {
        if (upstreamFormat == null || upstreamFormat.isBlank()) {
            return of(modelId);
        }
        return switch (upstreamFormat.trim().toLowerCase(Locale.ROOT)) {
            case "anthropic" -> ANTHROPIC;
            case "openai", "openai-chat", "openai_chat" -> OPENAI_CHAT;
            case "nova", "amazon-nova" -> NOVA;
            default -> UNSUPPORTED;
        };
    }

    /** 厂商段 + 完整模型 ID → 家族。 */
    private static BedrockModelFamily ofVendor(String vendor, String fullId) {
        if ("anthropic".equals(vendor)) {
            return ANTHROPIC;
        }
        if ("amazon".equals(vendor)) {
            // amazon.* 目前只有 Nova 系列是对话模型（Titan 等不支持）
            return fullId.contains(".nova") ? NOVA : UNSUPPORTED;
        }
        if (OPENAI_CHAT_VENDORS.contains(vendor)) {
            return OPENAI_CHAT;
        }
        return UNSUPPORTED;
    }

    /** 该家族是否被网关支持（可用于启动期校验）。 */
    public boolean isSupported() {
        return this != UNSUPPORTED;
    }

    /** 供日志与错误信息使用的可读名称。 */
    public String displayName() {
        return switch (this) {
            case ANTHROPIC -> "Anthropic Messages";
            case OPENAI_CHAT -> "OpenAI Chat Completions";
            case NOVA -> "Amazon Nova";
            case UNSUPPORTED -> "unsupported";
        };
    }

    /** 供文档与提示使用的可选值列表。 */
    public static List<String> supportedFormatNames() {
        return List.of("anthropic", "openai", "nova");
    }
}
