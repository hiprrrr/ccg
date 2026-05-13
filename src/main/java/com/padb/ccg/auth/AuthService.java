package com.padb.ccg.auth;

import com.padb.ccg.core.exception.UnauthorizedException;
import com.padb.ccg.core.model.AuthResult;
import com.padb.ccg.core.model.ModelAuthorization;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

import java.time.Instant;

/**
 * 认证服务，编排用户模型访问权限的校验流程。
 * 当 Mock 模式启用时直接放行，否则通过 Token 缓存或认证平台获取身份和授权信息并校验。
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
     * @param token          请求携带的认证 Token
     * @param requestedModel 请求的模型名称
     * @return 校验通过后的认证结果，失败时返回 {@link UnauthorizedException}
     */
    public Mono<AuthResult> authorize(String token, String requestedModel) {
        // Mock 模式：跳过认证，直接放行
        if (authProperties.mockEnabled()) {
            log.debug("Auth mock enabled — skipping authorization for token hash={} model='{}'", token.hashCode(), requestedModel);
            return Mono.just(new AuthResult(
                    token,
                    Instant.MAX,
                    java.util.List.of(new ModelAuthorization(requestedModel, Instant.MAX))));
        }

        return cacheManager.getAuthorization(token, requestedModel)
                .flatMap(authResult -> {
                    if (!authResult.tokenExpireAt().isAfter(Instant.now())) {
                        log.warn("Token expired for personId='{}'", authResult.personId());
                        return Mono.error(new UnauthorizedException("Token expired"));
                    }

                    // 查找匹配的模型授权
                    var match = authResult.models().stream()
                            .filter(a -> a.name().equals(requestedModel))
                            .findFirst();

                    if (match.isEmpty()) {
                        log.warn("Model '{}' not authorized for personId='{}'", requestedModel, authResult.personId());
                        return Mono.error(new UnauthorizedException(
                                "Model '" + requestedModel + "' not authorized for person '" + authResult.personId() + "'"));
                    }

                    // 检查授权是否已过期
                    if (!match.get().expireAt().isAfter(Instant.now())) {
                        log.warn("Authorization expired for model='{}' personId='{}'", requestedModel, authResult.personId());
                        return Mono.error(new UnauthorizedException(
                                "Authorization expired for model '" + requestedModel + "'"));
                    }

                    log.debug("Authorization OK: personId='{}' model='{}'", authResult.personId(), requestedModel);
                    return Mono.just(authResult);
                });
    }
}
