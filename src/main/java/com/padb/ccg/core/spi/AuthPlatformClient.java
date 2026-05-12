package com.padb.ccg.core.spi;

import com.padb.ccg.core.model.ModelAuthorization;
import reactor.core.publisher.Mono;

import java.util.List;

/**
 * 认证平台客户端接口，用于从外部认证平台获取用户的模型授权列表
 */
public interface AuthPlatformClient {

    /**
     * 获取指定用户已授权的模型列表
     *
     * @param username 用户名
     * @return 模型授权列表的 Mono，包含模型名称及过期时间
     */
    Mono<List<ModelAuthorization>> fetchAuthorizations(String username);
}
