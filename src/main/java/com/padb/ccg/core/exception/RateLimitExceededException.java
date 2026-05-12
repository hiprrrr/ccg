package com.padb.ccg.core.exception;

/**
 * 限流异常，表示用户请求频率超出限制，对应 HTTP 429
 */
public class RateLimitExceededException extends GatewayException {

    public RateLimitExceededException(String message) {
        super(message, 429);
    }
}
