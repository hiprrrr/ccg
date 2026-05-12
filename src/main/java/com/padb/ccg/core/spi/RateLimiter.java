package com.padb.ccg.core.spi;

import reactor.core.publisher.Mono;

/**
 * 限流器接口，基于令牌桶或计数窗口实现用户级别的请求频率控制
 */
public interface RateLimiter {

    /**
     * 尝试为用户获取一个请求配额
     *
     * @param username 用户名
     * @return true 表示允许通过，false 或 Mono.error 表示限流拒绝
     */
    Mono<Boolean> tryAcquire(String username);
}
