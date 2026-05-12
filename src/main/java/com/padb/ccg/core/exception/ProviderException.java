package com.padb.ccg.core.exception;

/**
 * 供应商异常，表示下游 Bedrock 调用失败，对应 HTTP 502
 */
public class ProviderException extends GatewayException {

    public ProviderException(String message) {
        super(message, 502);
    }
}
