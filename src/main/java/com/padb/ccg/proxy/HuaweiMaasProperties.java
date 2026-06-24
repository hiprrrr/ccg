package com.padb.ccg.proxy;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * 华为云 MaaS 配置属性，绑定 {@code huawei-maas.*}，与 {@link BedrockProperties} 同级。
 *
 * @param baseUrl        Anthropic 兼容 API 根地址（不含 {@code /v1/messages}）
 * @param apiKey         MaaS API Key，对应请求头 {@code x-api-key}
 * @param retryMax       最大重试次数
 * @param timeoutSeconds 单次调用超时（秒）
 */
@ConfigurationProperties(prefix = "huawei-maas")
public record HuaweiMaasProperties(String baseUrl, String apiKey, int retryMax, int timeoutSeconds) {

    public HuaweiMaasProperties {
        if (baseUrl == null || baseUrl.isBlank()) {
            baseUrl = "https://api.modelarts-maas.com/anthropic";
        }
        if (retryMax <= 0) {
            retryMax = 3;
        }
        if (timeoutSeconds <= 0) {
            timeoutSeconds = 300;
        }
    }
}
