package com.padb.ccg.proxy;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * 上游调用全局参数（第一层级配置 {@code upstream.*}），Bedrock 与 HTTP 透传共用。
 *
 * @param retryMax        最大重试次数
 * @param timeoutSeconds  上游调用超时（秒）
 */
@ConfigurationProperties(prefix = "upstream")
public record UpstreamProperties(int retryMax, int timeoutSeconds) {

    public UpstreamProperties {
        if (retryMax <= 0) {
            retryMax = 3;
        }
        if (timeoutSeconds <= 0) {
            timeoutSeconds = 300;
        }
    }
}
