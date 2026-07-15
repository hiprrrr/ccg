package com.padb.ccg.proxy;

/**
 * HTTP 透传上游 API 协议格式。
 */
public enum UpstreamApiFormat {
    /**
     * 未配置 {@code api-format}：按客户端协议透传（Anthropic↔Anthropic、OpenAI↔OpenAI），不做格式互转。
     */
    PASSTHROUGH,
    /** Anthropic Messages API：{@code /v1/messages}，鉴权 {@code x-api-key} */
    ANTHROPIC,
    /** OpenAI Chat Completions：{@code /chat/completions}，鉴权 {@code Authorization: Bearer} */
    OPENAI;

    /**
     * 从配置字符串解析。
     * <ul>
     *   <li>空 / 未配置 → {@link #PASSTHROUGH}</li>
     *   <li>{@code anthropic} → {@link #ANTHROPIC}</li>
     *   <li>{@code openai} → {@link #OPENAI}</li>
     * </ul>
     */
    public static UpstreamApiFormat fromConfig(String value) {
        if (value == null || value.isBlank()) {
            return PASSTHROUGH;
        }
        return switch (value.trim().toLowerCase()) {
            case "openai", "open_ai" -> OPENAI;
            case "anthropic" -> ANTHROPIC;
            case "passthrough", "none", "auto" -> PASSTHROUGH;
            default -> PASSTHROUGH;
        };
    }
}
