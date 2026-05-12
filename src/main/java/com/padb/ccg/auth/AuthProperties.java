package com.padb.ccg.auth;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * 认证配置属性，绑定 {@code auth.*} 配置项
 *
 * @param platformUrl             认证平台 URL
 * @param platformTimeoutSeconds  认证平台请求超时时间（秒），默认 5
 * @param cacheTtlSeconds         认证缓存 TTL（秒），默认 28800（8小时）
 * @param mockEnabled             是否启用 Mock 模式（本地开发用），开启后跳过认证
 */
@ConfigurationProperties(prefix = "auth")
public record AuthProperties(
    String platformUrl,
    int platformTimeoutSeconds,
    int cacheTtlSeconds,
    boolean mockEnabled
) {
    public AuthProperties {
        if (platformTimeoutSeconds <= 0) {
            platformTimeoutSeconds = 5;
        }
        if (cacheTtlSeconds <= 0) {
            cacheTtlSeconds = 28800;
        }
    }
}
