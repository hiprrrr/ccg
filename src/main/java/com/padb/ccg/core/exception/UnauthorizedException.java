package com.padb.ccg.core.exception;

/**
 * 未授权异常，表示用户无权访问请求的模型，对应 HTTP 403
 */
public class UnauthorizedException extends GatewayException {

    public UnauthorizedException(String message) {
        super(message, 403);
    }
}
