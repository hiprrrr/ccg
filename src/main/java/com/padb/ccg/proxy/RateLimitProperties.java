package com.padb.ccg.proxy;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * 限流配置属性，绑定 {@code rate-limit.*} 配置项
 *
 * @param defaultRpm 默认每分钟请求数（RPM）上限
 */
@ConfigurationProperties(prefix = "rate-limit")
public record RateLimitProperties(int defaultRpm) {}
