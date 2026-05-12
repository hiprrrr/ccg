package com.padb.ccg.auth;

import com.padb.ccg.core.exception.UnauthorizedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

import java.time.Instant;

/**
 * 认证服务，编排用户模型访问权限的校验流程。
 * 当 Mock 模式启用时直接放行，否则通过缓存或认证平台获取授权信息并校验。
 */
@Component
public class AuthService {

    private static final Logger log = LoggerFactory.getLogger(AuthService.class);

    private final AuthCacheManager cacheManager;
    private final AuthProperties authProperties;

    public AuthService(AuthCacheManager cacheManager, AuthProperties authProperties) {
        this.cacheManager = cacheManager;
        this.authProperties = authProperties;
    }

    /**
     * 校验用户是否有权访问指定模型
     *
     * @param username       用户名
     * @param requestedModel 请求的模型名称
     * @return 校验通过的 Mono，失败时返回 {@link UnauthorizedException}
     */
    public Mono<Void> authorize(String username, String requestedModel) {
        // Mock 模式：跳过认证，直接放行
        if (authProperties.mockEnabled()) {
            log.debug("Auth mock enabled — skipping authorization for user='{}' model='{}'", username, requestedModel);
            return Mono.empty();
        }

        return cacheManager.getAuthorizations(username)
                .flatMap(auths -> {
                    // 查找匹配的模型授权
                    var match = auths.stream()
                            .filter(a -> a.name().equals(requestedModel))
                            .findFirst();

                    if (match.isEmpty()) {
                        log.warn("Model '{}' not authorized for user='{}'", requestedModel, username);
                        return Mono.error(new UnauthorizedException(
                                "Model '" + requestedModel + "' not authorized for user '" + username + "'"));
                    }

                    // 检查授权是否已过期
                    if (match.get().expireAt().isBefore(Instant.now())) {
                        log.warn("Authorization expired for model='{}' user='{}'", requestedModel, username);
                        return Mono.error(new UnauthorizedException(
                                "Authorization expired for model '" + requestedModel + "'"));
                    }

                    log.debug("Authorization OK: user='{}' model='{}'", username, requestedModel);
                    return Mono.empty();
                });
    }
}
