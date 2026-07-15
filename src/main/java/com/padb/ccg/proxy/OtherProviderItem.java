package com.padb.ccg.proxy;

import java.util.List;

/**
 * {@code other-providers} 列表中的单项：与 AWS 平行的 HTTP 透传供应商。
 *
 * @param name       供应商名称，供 {@code model-mappings.provider} 引用
 * @param baseUrl    上游 API 根地址（Anthropic 路径不含 {@code /v1/messages}，OpenAI 路径不含 {@code /chat/completions}）
 * @param apiKeys    上游 API Key 列表（1 个或多个）；请求时在有效 key 中均匀随机选取
 * @param apiFormat  可选。{@code anthropic} / {@code openai}；未配置则按客户端协议完全透传（不做格式互转）
 */
public record OtherProviderItem(String name, String baseUrl, List<String> apiKeys, String apiFormat) {

    public OtherProviderItem {
        if (name != null) {
            name = name.trim();
        }
        if (apiKeys == null) {
            apiKeys = List.of();
        } else {
            apiKeys = List.copyOf(apiKeys);
        }
    }

    /** 返回非空的 api-key 列表（保持 YAML 顺序） */
    public List<String> eligibleApiKeys() {
        return apiKeys.stream()
                .filter(k -> k != null && !k.isBlank())
                .toList();
    }

    /** 解析后的上游 API 格式 */
    public UpstreamApiFormat resolvedApiFormat() {
        return UpstreamApiFormat.fromConfig(apiFormat);
    }

    public boolean isOpenAiFormat() {
        return resolvedApiFormat() == UpstreamApiFormat.OPENAI;
    }

    public boolean isAnthropicFormat() {
        return resolvedApiFormat() == UpstreamApiFormat.ANTHROPIC;
    }

    /** 未配置 api-format：客户端侧 Anthropic/OpenAI 各自透传，不做互转 */
    public boolean isPassthroughFormat() {
        return resolvedApiFormat() == UpstreamApiFormat.PASSTHROUGH;
    }

    /**
     * OpenAI 客户端是否可直连上游透传（显式 openai，或未配置 api-format）。
     */
    public boolean supportsOpenAiClientPassthrough() {
        return isOpenAiFormat() || isPassthroughFormat();
    }

    /**
     * 解析实际上游 base URL：去掉尾部斜杠；未配置时须显式配置。
     */
    public String resolvedBaseUrl() {
        if (baseUrl == null || baseUrl.isBlank()) {
            throw new IllegalStateException(
                    "other-providers entry '" + name + "' requires base-url");
        }
        return trimTrailingSlash(baseUrl);
    }

    private static String trimTrailingSlash(String url) {
        String trimmed = url.trim();
        while (trimmed.endsWith("/")) {
            trimmed = trimmed.substring(0, trimmed.length() - 1);
        }
        return trimmed;
    }
}
