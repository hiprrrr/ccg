package com.padb.ccg.core.exception;

/**
 * 供应商异常，表示下游 Bedrock 调用失败，对应 HTTP 502
 */
public class ProviderException extends GatewayException {

    /** 上游返回的 HTTP 状态码；非 HTTP 层错误为 0 */
    private final int upstreamStatus;

    public ProviderException(String message) {
        this(message, 0);
    }

    public ProviderException(String message, int upstreamStatus) {
        super(message, 502);
        this.upstreamStatus = upstreamStatus;
    }

    /** 上游返回的 HTTP 状态码（如 401/403/429）；无则为 0 */
    public int upstreamStatus() {
        return upstreamStatus;
    }
}
