package com.padb.ccg.proxy;

/**
 * 华为云 MaaS 上游 API 协议格式。
 */
public enum HuaweiMaasApiFormat {
    /** Anthropic Messages API：{@code /v1/messages}，鉴权 {@code x-api-key} */
    ANTHROPIC,
    /** OpenAI Chat Completions：{@code /chat/completions}，鉴权 {@code Authorization: Bearer} */
    OPENAI;

    /**
     * 从配置字符串解析（{@code anthropic} / {@code openai}，大小写不敏感）。
     */
    public static HuaweiMaasApiFormat fromConfig(String value) {
        if (value == null || value.isBlank()) {
            return ANTHROPIC;
        }
        return switch (value.trim().toLowerCase()) {
            case "openai", "open_ai" -> OPENAI;
            default -> ANTHROPIC;
        };
    }
}
