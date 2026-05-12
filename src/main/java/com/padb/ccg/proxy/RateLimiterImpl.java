package com.padb.ccg.proxy;

import com.padb.ccg.core.exception.RateLimitExceededException;
import com.padb.ccg.core.spi.RateLimiter;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 基于滑动计数窗口的限流器实现。
 * 每个用户维护一个分钟级窗口，窗口内计数超过 RPM 阈值时拒绝请求。
 * 用户窗口在 5 分钟无访问后自动过期清理。
 */
@Component
public class RateLimiterImpl implements RateLimiter {

    /** 用户窗口过期时间（分钟），无访问时自动清理 */
    private static final int WINDOW_EXPIRE_MINUTES = 5;

    private final RateLimitConfigHolder configHolder;

    /** 用户级别的限流窗口缓存，key 为用户名 */
    private final Cache<String, UserRateWindow> userWindows;

    public RateLimiterImpl(RateLimitConfigHolder configHolder) {
        this.configHolder = configHolder;
        this.userWindows = Caffeine.newBuilder()
                .expireAfterAccess(WINDOW_EXPIRE_MINUTES, TimeUnit.MINUTES)
                .build();
    }

    @Override
    public Mono<Boolean> tryAcquire(String username) {
        // 获取或创建用户限流窗口
        UserRateWindow window = userWindows.get(username, k -> new UserRateWindow());
        // 计算当前分钟窗口的标识（Unix 时间戳 / 60000）
        long now = System.currentTimeMillis() / 60_000;

        synchronized (window) {
            // 进入新窗口时重置计数器
            if (window.windowStart != now) {
                window.counter.set(0);
                window.windowStart = now;
            }
            long count = window.counter.incrementAndGet();
            int rpm = configHolder.getDefaultRpm();
            if (count > rpm) {
                return Mono.error(new RateLimitExceededException(
                        "Rate limit exceeded for user '" + username + "': " + rpm + " RPM"));
            }
        }
        return Mono.just(true);
    }

    /**
     * 用户限流窗口，记录当前分钟窗口的起始时间和请求计数
     */
    private static class UserRateWindow {
        /** 当前窗口内的请求计数 */
        final AtomicLong counter = new AtomicLong(0);
        /** 当前窗口起始时间（分钟级 Unix 时间戳） */
        volatile long windowStart;
    }
}
