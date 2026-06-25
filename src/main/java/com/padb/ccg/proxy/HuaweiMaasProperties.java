package com.padb.ccg.proxy;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * 华为云 MaaS 配置属性，绑定 {@code huawei-maas.*}，与 {@link BedrockProperties} 同级。
 *
 * @param baseUrl        上游 API 根地址（Anthropic 不含 {@code /v1/messages}，OpenAI 不含 {@code /chat/completions}）
 * @param apiKey         MaaS API Key
 * @param apiFormat      上游协议：{@code anthropic} 或 {@code openai}
 * @param retryMax       最大重试次数
 * @param timeoutSeconds 单次调用超时（秒）
 */
@ConfigurationProperties(prefix = "huawei-maas")
public record HuaweiMaasProperties(String baseUrl, String apiKey, String apiFormat,
                                   int retryMax, int timeoutSeconds) {

    private static final String DEFAULT_ANTHROPIC_BASE = "https://api.modelarts-maas.com/anthropic";
    private static final String DEFAULT_OPENAI_BASE = "https://api.modelarts-maas.com/openai/v1";

    public HuaweiMaasProperties {
        if (retryMax <= 0) {
            retryMax = 3;
        }
        if (timeoutSeconds <= 0) {
            timeoutSeconds = 300;
        }
    }

    /** 解析后的上游 API 格式 */
    public HuaweiMaasApiFormat resolvedApiFormat() {
        return HuaweiMaasApiFormat.fromConfig(apiFormat);
    }

    public boolean isOpenAiFormat() {
        return resolvedApiFormat() == HuaweiMaasApiFormat.OPENAI;
    }

    public boolean isAnthropicFormat() {
        return resolvedApiFormat() == HuaweiMaasApiFormat.ANTHROPIC;
    }

    /**
     * 解析实际上游 base URL：显式配置优先，否则按 {@code api-format} 选择默认域名路径。
     */
    public String resolvedBaseUrl() {
        if (baseUrl != null && !baseUrl.isBlank()) {
            return trimTrailingSlash(baseUrl);
        }
        return isOpenAiFormat() ? DEFAULT_OPENAI_BASE : DEFAULT_ANTHROPIC_BASE;
    }

    private static String trimTrailingSlash(String url) {
        String trimmed = url.trim();
        while (trimmed.endsWith("/")) {
            trimmed = trimmed.substring(0, trimmed.length() - 1);
        }
        return trimmed;
    }
}
