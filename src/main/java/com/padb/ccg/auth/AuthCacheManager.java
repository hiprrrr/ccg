package com.padb.ccg.auth;

import com.padb.ccg.core.model.ModelAuthorization;
import com.padb.ccg.core.exception.ProviderException;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Expiry;
import org.checkerframework.checker.index.qual.NonNegative;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * 认证缓存管理器，提供多层缓存策略以避免重复调用认证平台。
 *
 * 缓存策略：
 * 1. Caffeine 变长过期缓存 — 按模型最短过期时间动态设置 TTL
 * 2. 过期缓存（staleMap）— 认证平台不可用时降级使用上一次的结果
 * 3. 进行中请求合并（inflight）— 同一用户的并发请求共享一次平台调用
 */
@Component
public class AuthCacheManager {

    private static final Logger log = LoggerFactory.getLogger(AuthCacheManager.class);

    /** 平台故障时，过期缓存的重新生效时长 */
    private static final Duration STALE_RECACHE_TTL = Duration.ofMinutes(1);

    private final AuthPlatformClientImpl platformClient;
    private final AuthProperties props;

    /** 进行中的请求映射，key 为用户名，value 为异步结果 Future */
    private final ConcurrentHashMap<String, CompletableFuture<List<ModelAuthorization>>> inflight;

    /** Caffeine 变长过期缓存，TTL 取各模型授权的最短过期时间 */
    private final Cache<String, List<ModelAuthorization>> cache;

    /** 过期缓存副本，认证平台故障时降级使用 */
    private final ConcurrentHashMap<String, List<ModelAuthorization>> staleMap;

    public AuthCacheManager(AuthPlatformClientImpl platformClient, AuthProperties props) {
        this.platformClient = platformClient;
        this.props = props;
        this.inflight = new ConcurrentHashMap<>();
        this.staleMap = new ConcurrentHashMap<>();

        // 构建基于变长过期策略的 Caffeine 缓存
        this.cache = Caffeine.newBuilder()
                .expireAfter(new Expiry<String, List<ModelAuthorization>>() {
                    @Override
                    public long expireAfterCreate(String key, List<ModelAuthorization> value, long currentTime) {
                        return toNanos(computeTtl(value));
                    }

                    @Override
                    public long expireAfterUpdate(String key, List<ModelAuthorization> value,
                                                  long currentTime, @NonNegative long currentDuration) {
                        return toNanos(computeTtl(value));
                    }

                    @Override
                    public long expireAfterRead(String key, List<ModelAuthorization> value,
                                                long currentTime, @NonNegative long currentDuration) {
                        // 读取后保持原有过期时间，不重置
                        return currentDuration;
                    }
                })
                .build();
    }

    /**
     * 计算授权列表的 TTL：取所有授权中最早的过期时间，
     * 并与默认 TTL 比较，取较小值。
     * 若授权永不过期（Instant.MAX），则使用默认 TTL。
     */
    private Duration computeTtl(List<ModelAuthorization> auths) {
        Duration defaultTtl = Duration.ofSeconds(props.cacheTtlSeconds());
        Instant now = Instant.now();
        return auths.stream()
                .map(ModelAuthorization::expireAt)
                .filter(expireAt -> expireAt != null && !expireAt.equals(Instant.MAX))
                .map(expireAt -> Duration.between(now, expireAt))
                .filter(d -> !d.isNegative())
                .min(Duration::compareTo)
                .filter(d -> d.compareTo(defaultTtl) < 0)
                .orElse(defaultTtl);
    }

    /** Duration 转纳秒 */
    private long toNanos(Duration d) {
        return d.toNanos();
    }

    /**
     * 获取用户的模型授权列表，优先从缓存读取，缓存未命中时调用认证平台。
     * 同一用户的并发请求会合并为一次远程调用（inflight 去重）。
     */
    public Mono<List<ModelAuthorization>> getAuthorizations(String username) {
        // 一级缓存：Caffeine 变长过期缓存
        List<ModelAuthorization> cached = cache.getIfPresent(username);
        if (cached != null) {
            return Mono.just(cached);
        }

        // 二级去重：检查是否已有进行中的请求
        // 使用 get + putIfAbsent 而非 computeIfAbsent，避免在 subscribe 回调同步触发时递归更新
        CompletableFuture<List<ModelAuthorization>> inflightCf = inflight.get(username);
        if (inflightCf != null) {
            return Mono.fromFuture(inflightCf)
                    .doOnCancel(() -> inflight.remove(username));
        }

        CompletableFuture<List<ModelAuthorization>> future = new CompletableFuture<>();
        inflightCf = inflight.putIfAbsent(username, future);
        if (inflightCf != null) {
            // 并发情况下其他线程已创建了 inflight，直接复用
            return Mono.fromFuture(inflightCf)
                    .doOnCancel(() -> inflight.remove(username));
        }

        log.info("Fetching authorizations from platform for user={}", username);
        platformClient.fetchAuthorizations(username)
                .subscribe(
                        result -> {
                            // 成功回调：清理 inflight，写入缓存和过期缓存
                            inflight.remove(username);
                            cache.put(username, result);
                            staleMap.put(username, result);
                            future.complete(result);
                        },
                        error -> {
                            // 失败回调：尝试降级使用过期缓存
                            log.warn("Auth platform error for user={}, trying stale cache", username, error);
                            inflight.remove(username);
                            List<ModelAuthorization> stale = staleMap.get(username);
                            if (stale != null) {
                                log.info("Using stale cache for user={}", username);
                                // 将过期缓存重新写入 Caffeine，设较短 TTL
                                cache.policy().expireVariably()
                                        .ifPresent(p -> p.put(username, stale,
                                                STALE_RECACHE_TTL.toNanos(), TimeUnit.NANOSECONDS));
                                future.complete(stale);
                            } else {
                                future.completeExceptionally(
                                        new ProviderException("Unable to verify authorization: auth platform unavailable"));
                            }
                        });

        return Mono.fromFuture(future)
                .doOnCancel(() -> {
                    // 客户端取消时清理 inflight 和 Future
                    inflight.remove(username);
                    future.cancel(true);
                });
    }
}
