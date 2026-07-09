package com.padb.ccg.core.model;

/**
 * 模型映射下的上游账号凭证，用于多账号随机分流，降低单 Key 被限流风险。
 *
 * @param id           账号标识（用于日志，未配置时由选择器自动生成）
 * @param apiKey       华为 MaaS API Key（{@link ProviderChannel#HUAWEI}）
 * @param accessKey    AWS Access Key（{@link ProviderChannel#AWS}）
 * @param secretKey    AWS Secret Key
 * @param sessionToken AWS STS 会话令牌（可选）
 * @param region       单账号 AWS 区域覆盖（可选，未配置则用模型映射或全局默认区域）
 */
public record ProviderAccount(String id, String apiKey, String accessKey, String secretKey,
                               String sessionToken, String region) {

    /** 华为账号是否配置了可用 API Key */
    public boolean hasHuaweiApiKey() {
        return apiKey != null && !apiKey.isBlank();
    }

    /** AWS 账号是否配置了静态密钥对 */
    public boolean hasAwsCredentials() {
        return accessKey != null && !accessKey.isBlank()
                && secretKey != null && !secretKey.isBlank();
    }

    /** 解析用于日志的账号 ID，空则返回 fallback */
    public String resolvedId(String fallback) {
        if (id != null && !id.isBlank()) {
            return id;
        }
        return fallback;
    }
}
