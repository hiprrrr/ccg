package com.padb.ccg.auth;

import com.padb.ccg.core.exception.AuthPlatformUnavailableException;
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
import java.util.concurrent.atomic.AtomicInteger;

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

    /** 进行中的请求映射，key 为认证 Token */
    private final ConcurrentHashMap<String, Inflight> inflight;

    /** Caffeine 变长过期缓存，key 为认证 Token */
    private final Cache<String, AuthResult> cache;

    /**
     * 共享一次平台调用的 inflight 句柄。
     * subscribers 记录当前等待该调用的订阅者数量：单个订阅者取消不得影响其他等待者，
     * 计数归零且调用未完成时才允许从 inflight 中清理，避免「已取消的孤儿调用」永久阻塞后续请求。
     */
    private record Inflight(CompletableFuture<AuthResult> future, AtomicInteger subscribers) {
        static Inflight create() {
            return new Inflight(new CompletableFuture<>(), new AtomicInteger(1));
        }
    }

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
     * 否则重新调用认证平台。同一 Token 的并发请求共享一次远程调用；
     * 单个订阅者取消不影响其他等待者，全部取消后才清理 inflight 允许重试。
     */
    public Mono<AuthResult> getAuthorization(String token, String requestedModel) {
        // 一级缓存：Caffeine 变长过期缓存
        AuthResult cached = cache.getIfPresent(token);
        if (cached != null && isUsable(cached, requestedModel)) {
            return Mono.just(cached);
        }

        // 二级去重：原子地 retain 存活 holder，或创建新 holder 并发起平台调用。
        // 在 compute 内检查 subscribers>0，避免与 release 的归零清理产生「复活」竞态
        Inflight[] created = new Inflight[1];
        Inflight holder = inflight.compute(token, (k, cur) -> {
            if (cur != null && cur.subscribers().get() > 0) {
                cur.subscribers().incrementAndGet();
                return cur;
            }
            created[0] = Inflight.create();
            return created[0];
        });

        if (created[0] != null) {
            fetchFromPlatform(token, created[0]);
        }

        // suppressCancel=true：单个订阅者取消不得传播到底层共享 Future
        return Mono.fromFuture(holder.future(), true)
                .doOnCancel(() -> release(token, holder));
    }

    /** 调用认证平台并回填共享 Future；完成或失败时按 holder 身份清理 inflight，避免误删新 holder */
    private void fetchFromPlatform(String token, Inflight holder) {
        log.info("Fetching authorization from platform for token hash={}", token.hashCode());
        platformClient.fetchAuthorization(token)
                .subscribe(
                        result -> {
                            // 成功回调：清理 inflight，写入缓存
                            inflight.remove(token, holder);
                            cache.put(token, result);
                            holder.future().complete(result);
                        },
                        error -> {
                            // 认证接口失败时拒绝请求，不使用过期缓存降级
                            log.warn("Auth platform error for token hash={}", token.hashCode(), error);
                            inflight.remove(token, holder);
                            holder.future().completeExceptionally(
                                    new AuthPlatformUnavailableException("Unable to verify authorization: auth platform unavailable"));
                        });
    }

    /**
     * 订阅者取消时递减计数；归零且调用未完成时清理 inflight，使后续请求可以重新发起调用。
     * 不 cancel 共享 Future：平台调用自然完成后写入缓存，对后续请求反而有利。
     */
    private void release(String token, Inflight holder) {
        if (holder.subscribers().decrementAndGet() <= 0) {
            inflight.compute(token, (k, cur) ->
                    cur == holder && cur.subscribers().get() <= 0 ? null : cur);
        }
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
