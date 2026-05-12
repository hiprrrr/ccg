package com.padb.ccg.proxy;

import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * 限流配置持有者，线程安全地维护当前生效的默认 RPM 值。
 * 使用 {@link AtomicInteger} 支持配置热更新。
 */
@Component
public class RateLimitConfigHolder {

    private static final Logger log = LoggerFactory.getLogger(RateLimitConfigHolder.class);

    private final RateLimitProperties props;

    /** 当前生效的默认 RPM，原子更新，保证线程安全 */
    private final AtomicInteger defaultRpm = new AtomicInteger(60);

    public RateLimitConfigHolder(RateLimitProperties props) {
        this.props = props;
    }

    /** 启动时从配置初始化默认 RPM */
    @PostConstruct
    void init() {
        defaultRpm.set(props.defaultRpm());
        log.info("Rate limit default RPM initialized to {}", props.defaultRpm());
    }

    /** 获取当前生效的默认 RPM */
    public int getDefaultRpm() {
        return defaultRpm.get();
    }
}
