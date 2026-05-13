package com.padb.ccg.auth;

import com.padb.ccg.core.exception.ProviderException;
import com.padb.ccg.core.model.AuthResult;
import com.padb.ccg.core.model.ModelAuthorization;
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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 认证缓存管理器，提供 Token 维度的认证结果缓存以避免重复调用认证平台。
 *
 * 缓存策略：
 * 1. Caffeine 变长过期缓存 — TTL 取 Token 过期时间、模型最短过期时间和默认 TTL 的最小值
 * 2. 进行中请求合并（inflight）— 同一 Token 的并发请求共享一次平台调用
 * 3. 缓存缺失、Token 过期、请求模型缺失或模型过期时重新请求认证平台
 */
@Component
public class AuthCacheManager {

    private static final Logger log = LoggerFactory.getLogger(AuthCacheManager.class);

    private final AuthPlatformClientImpl platformClient;
    private final AuthProperties props;

    /** 进行中的请求映射，key 为认证 Token，value 为异步结果 Future */
    private final ConcurrentHashMap<String, CompletableFuture<AuthResult>> inflight;

    /** Caffeine 变长过期缓存，key 为认证 Token */
    private final Cache<String, AuthResult> cache;

    public AuthCacheManager(AuthPlatformClientImpl platformClient, AuthProperties props) {
        this.platformClient = platformClient;
        this.props = props;
        this.inflight = new ConcurrentHashMap<>();

        // 构建基于变长过期策略的 Caffeine 缓存
        this.cache = Caffeine.newBuilder()
                .expireAfter(new Expiry<String, AuthResult>() {
                    @Override
                    public long expireAfterCreate(String key, AuthResult value, long currentTime) {
                        return toNanos(computeTtl(value));
                    }

                    @Override
                    public long expireAfterUpdate(String key, AuthResult value,
                                                  long currentTime, @NonNegative long currentDuration) {
                        return toNanos(computeTtl(value));
                    }

                    @Override
                    public long expireAfterRead(String key, AuthResult value,
                                                long currentTime, @NonNegative long currentDuration) {
                        // 读取后保持原有过期时间，不重置
                        return currentDuration;
                    }
                })
                .build();
    }

    /**
     * 计算认证结果的 TTL：取 Token 过期时间、所有模型授权中最早的过期时间和默认 TTL 的最小值。
     */
    private Duration computeTtl(AuthResult authResult) {
        Duration defaultTtl = Duration.ofSeconds(props.cacheTtlSeconds());
        Instant now = Instant.now();
        Duration tokenTtl = Duration.between(now, authResult.tokenExpireAt());
        Duration modelTtl = authResult.models().stream()
                .map(ModelAuthorization::expireAt)
                .filter(expireAt -> expireAt != null && !expireAt.equals(Instant.MAX))
                .map(expireAt -> Duration.between(now, expireAt))
                .filter(d -> !d.isNegative())
                .min(Duration::compareTo)
                .orElse(defaultTtl);

        Duration ttl = min(defaultTtl, tokenTtl, modelTtl);
        return ttl.isNegative() || ttl.isZero() ? Duration.ofNanos(1) : ttl;
    }

    /** Duration 转纳秒 */
    private long toNanos(Duration d) {
        return d.toNanos();
    }

    /** 返回多个 Duration 中最小的值 */
    private Duration min(Duration first, Duration second, Duration third) {
        Duration min = first.compareTo(second) <= 0 ? first : second;
        return min.compareTo(third) <= 0 ? min : third;
    }

    /**
     * 获取 Token 的认证结果，缓存可用且包含未过期的请求模型时直接返回，
     * 否则重新调用认证平台。同一 Token 的并发请求会合并为一次远程调用。
     */
    public Mono<AuthResult> getAuthorization(String token, String requestedModel) {
        // 一级缓存：Caffeine 变长过期缓存
        AuthResult cached = cache.getIfPresent(token);
        if (cached != null && isUsable(cached, requestedModel)) {
            return Mono.just(cached);
        }

        // 二级去重：检查是否已有进行中的请求
        // 使用 get + putIfAbsent 而非 computeIfAbsent，避免在 subscribe 回调同步触发时递归更新
        CompletableFuture<AuthResult> inflightCf = inflight.get(token);
        if (inflightCf != null) {
            return Mono.fromFuture(inflightCf)
                    .doOnCancel(() -> inflight.remove(token));
        }

        CompletableFuture<AuthResult> future = new CompletableFuture<>();
        inflightCf = inflight.putIfAbsent(token, future);
        if (inflightCf != null) {
            // 并发情况下其他线程已创建了 inflight，直接复用
            return Mono.fromFuture(inflightCf)
                    .doOnCancel(() -> inflight.remove(token));
        }

        log.info("Fetching authorization from platform for token hash={}", token.hashCode());
        platformClient.fetchAuthorization(token)
                .subscribe(
                        result -> {
                            // 成功回调：清理 inflight，写入缓存
                            inflight.remove(token);
                            cache.put(token, result);
                            future.complete(result);
                        },
                        error -> {
                            // 认证接口失败时拒绝请求，不使用过期缓存降级
                            log.warn("Auth platform error for token hash={}", token.hashCode(), error);
                            inflight.remove(token);
                            future.completeExceptionally(
                                    new ProviderException("Unable to verify authorization: auth platform unavailable"));
                        });

        return Mono.fromFuture(future)
                .doOnCancel(() -> {
                    // 客户端取消时清理 inflight 和 Future
                    inflight.remove(token);
                    future.cancel(true);
                });
    }

    /** 判断缓存的认证结果是否仍可用于本次请求模型 */
    private boolean isUsable(AuthResult authResult, String requestedModel) {
        Instant now = Instant.now();
        if (!authResult.tokenExpireAt().isAfter(now)) {
            return false;
        }
        return authResult.models().stream()
                .anyMatch(model -> model.name().equals(requestedModel) && model.expireAt().isAfter(now));
    }
}
