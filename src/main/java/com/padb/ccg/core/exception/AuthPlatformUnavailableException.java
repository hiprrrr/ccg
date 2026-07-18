package com.padb.ccg.core.exception;

/**
 * 认证平台不可用异常，对应 HTTP 503。
 * 与 {@link ProviderException}（上游 LLM 供应商故障，502）区分，便于排查故障源。
 */
public class AuthPlatformUnavailableException extends GatewayException {

    public AuthPlatformUnavailableException(String message) {
        super(message, 503);
    }
}
