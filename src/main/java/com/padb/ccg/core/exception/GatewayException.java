package com.padb.ccg.core.exception;

/**
 * 网关基础异常类，所有业务异常均继承此类。
 * 携带 HTTP 状态码，由 {@code GlobalErrorHandler} 统一转换为 Anthropic 兼容的错误 JSON。
 */
public abstract class GatewayException extends RuntimeException {

    /** 对应的 HTTP 状态码 */
    private final int httpStatus;

    protected GatewayException(String message, int httpStatus) {
        super(message);
        this.httpStatus = httpStatus;
    }

    /** 获取异常对应的 HTTP 状态码 */
    public int getHttpStatus() {
        return httpStatus;
    }
}
