package com.padb.ccg.proxy;

import com.padb.ccg.core.exception.RateLimitExceededException;
import com.padb.ccg.core.spi.RateLimiter;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;

/**
 * 基于加权滑动窗口计数的限流器实现。
 * 预估值 = 当前窗口计数 + 上一窗口计数 × (1 - 当前窗口已过去比例)，
 * 相比固定窗口可抑制窗口边界处约 2×RPM 的突发流量。
 * 每个用户仅保存两个计数器（O(1) 内存），窗口在 5 分钟无访问后自动过期清理。
 *
 * 注意：限流为单实例内存实现，K8s 多副本部署时实际限额为 副本数 × RPM。
 */
@Component
public class RateLimiterImpl implements RateLimiter {

    /** 用户窗口过期时间（分钟），无访问时自动清理 */
    private static final int WINDOW_EXPIRE_MINUTES = 5;
    /** 窗口长度（毫秒），固定一分钟 */
    private static final long WINDOW_MILLIS = 60_000;

    private final RateLimitConfigHolder configHolder;

    /** 用户级别的限流窗口缓存，key 为用户名 */
    private final Cache<String, UserRateWindow> userWindows;

    /** 毫秒时钟，测试可替换以控制窗口边界 */
    private final LongSupplier millisSupplier;

    public RateLimiterImpl(RateLimitConfigHolder configHolder) {
        this(configHolder, System::currentTimeMillis);
    }

    /** 测试专用：注入可控时钟 */
    RateLimiterImpl(RateLimitConfigHolder configHolder, LongSupplier millisSupplier) {
        this.configHolder = configHolder;
        this.millisSupplier = millisSupplier;
        this.userWindows = Caffeine.newBuilder()
                .expireAfterAccess(WINDOW_EXPIRE_MINUTES, TimeUnit.MINUTES)
                .build();
    }

    @Override
    public Mono<Boolean> tryAcquire(String username) {
        UserRateWindow window = userWindows.get(username, k -> new UserRateWindow());
        long nowMs = millisSupplier.getAsLong();
        long now = nowMs / WINDOW_MILLIS;

        synchronized (window) {
            // 窗口滚动：上一窗口计数仅在相邻时参与加权，跨多窗口空闲后清零
            if (window.windowStart != now) {
                window.previousCount = (window.windowStart == now - 1) ? window.currentCount : 0;
                window.currentCount = 0;
                window.windowStart = now;
            }
            double elapsedRatio = (nowMs % WINDOW_MILLIS) / (double) WINDOW_MILLIS;
            double estimated = window.previousCount * (1 - elapsedRatio) + window.currentCount + 1;
            int rpm = configHolder.getDefaultRpm();
            if (estimated > rpm) {
                return Mono.error(new RateLimitExceededException(
                        "Rate limit exceeded for user '" + username + "': " + rpm + " RPM"));
            }
            window.currentCount++;
        }
        return Mono.just(true);
    }

    /**
     * 用户限流窗口：上一分钟窗口计数（用于加权）+ 当前分钟窗口计数。
     * 所有字段仅在 synchronized(window) 内访问。
     */
    private static class UserRateWindow {
        /** 上一分钟窗口的请求总数 */
        long previousCount;
        /** 当前分钟窗口已计数请求数 */
        long currentCount;
        /** 当前窗口起始时间（分钟级 Unix 时间戳） */
        long windowStart;
    }
}
