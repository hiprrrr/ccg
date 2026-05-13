package com.padb.ccg.core.spi;

import com.padb.ccg.core.model.AuthResult;
import reactor.core.publisher.Mono;

/**
 * 认证平台客户端接口，用于从外部认证平台根据 Token 获取个人身份和模型授权列表
 */
public interface AuthPlatformClient {

    /**
     * 获取指定 Token 对应的个人身份和授权模型列表
     *
     * @param token 请求携带的认证 Token
     * @return 认证结果的 Mono，包含个人 ID、Token 过期时间和模型授权列表
     */
    Mono<AuthResult> fetchAuthorization(String token);
}
