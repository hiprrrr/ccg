package com.padb.ccg.core.model;

/**
 * 华为 MaaS 模型映射下的多 Key 凭证，用于随机分流，降低单 Key 被限流风险。
 *
 * @param id     账号标识（用于日志，未配置时由选择器自动生成）
 * @param apiKey 华为 MaaS API Key
 */
public record ProviderAccount(String id, String apiKey) {

    /** 是否配置了可用 API Key */
    public boolean hasApiKey() {
        return apiKey != null && !apiKey.isBlank();
    }

    /** 解析用于日志的账号 ID，空则返回 fallback */
    public String resolvedId(String fallback) {
        if (id != null && !id.isBlank()) {
            return id;
        }
        return fallback;
    }
}
