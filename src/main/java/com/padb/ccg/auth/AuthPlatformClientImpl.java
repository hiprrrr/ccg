package com.padb.ccg.auth;

import com.padb.ccg.core.exception.ProviderException;
import com.padb.ccg.core.model.AuthResult;
import com.padb.ccg.core.model.ModelAuthorization;
import com.padb.ccg.core.spi.AuthPlatformClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;
import reactor.netty.http.client.HttpClient;

import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.List;
import java.util.Map;

/**
 * 认证平台客户端实现，通过 HTTP 调用外部认证平台获取用户模型授权列表。
 * 使用 WebClient（Reactor Netty）进行非阻塞 HTTP 通信。
 */
@Component
public class AuthPlatformClientImpl implements AuthPlatformClient {

    private static final Logger log = LoggerFactory.getLogger(AuthPlatformClientImpl.class);

    /** 连接超时时间（毫秒） */
    private static final int CONNECT_TIMEOUT_MILLIS = 5_000;

    private final WebClient webClient;
    private final AuthProperties props;

    public AuthPlatformClientImpl(WebClient.Builder webClientBuilder,
                                   AuthProperties props) {
        this.props = props;

        // 配置 Netty HTTP 客户端的连接超时和响应超时
        var httpClient = HttpClient.create()
                .option(io.netty.channel.ChannelOption.CONNECT_TIMEOUT_MILLIS, CONNECT_TIMEOUT_MILLIS)
                .responseTimeout(Duration.ofSeconds(props.platformTimeoutSeconds() + 2));

        this.webClient = webClientBuilder
                .clientConnector(new org.springframework.http.client.reactive.ReactorClientHttpConnector(httpClient))
                .build();
    }

    @Override
    public Mono<AuthResult> fetchAuthorization(String token) {
        URI uri = URI.create(props.platformUrl() + "?token=" + encode(token));

        return webClient.get()
                .uri(uri)
                .retrieve()
                // 4xx 客户端错误 → 转为 ProviderException
                .onStatus(s -> s.is4xxClientError(), response -> {
                    log.warn("Auth platform returned client error {} for token hash={}", response.statusCode(), token.hashCode());
                    return Mono.error(new ProviderException(
                            "Auth platform client error: " + response.statusCode()));
                })
                // 5xx 服务端错误 → 转为 ProviderException
                .onStatus(s -> s.is5xxServerError(), response -> {
                    log.warn("Auth platform returned server error {} for token hash={}", response.statusCode(), token.hashCode());
                    return Mono.error(new ProviderException(
                            "Auth platform server error: " + response.statusCode()));
                })
                .bodyToMono(Map.class)
                .timeout(Duration.ofSeconds(props.platformTimeoutSeconds()))
                .map(body -> parseAuthorization(body, token));
    }

    /** URL 编码工具方法 */
    private static String encode(String value) {
        return java.net.URLEncoder.encode(value, java.nio.charset.StandardCharsets.UTF_8);
    }

    /**
     * 解析认证平台返回的 JSON 响应，提取个人身份、Token 过期时间和模型授权列表。
     * 预期格式: {"person_id":"...","token_expire_at":"ISO-8601","models":[{"name":"...","expire_at":"ISO-8601"}]}
     */
    @SuppressWarnings("unchecked")
    private AuthResult parseAuthorization(Map<?, ?> body, String token) {
        String personId = stringValue(body.get("person_id"));
        if (personId == null || personId.isBlank()) {
            throw new ProviderException("Auth platform response missing person_id");
        }

        String tokenExpireAtStr = stringValue(body.get("token_expire_at"));
        if (tokenExpireAtStr == null || tokenExpireAtStr.isBlank()) {
            throw new ProviderException("Auth platform response missing token_expire_at");
        }

        Instant tokenExpireAt;
        try {
            tokenExpireAt = Instant.parse(tokenExpireAtStr);
        } catch (DateTimeParseException e) {
            throw new ProviderException("Auth platform response has invalid token_expire_at");
        }

        Object modelsObj = body.get("models");
        if (!(modelsObj instanceof List<?> list)) {
            log.warn("Auth platform returned unexpected models format for token hash={}", token.hashCode());
            return new AuthResult(personId, tokenExpireAt, List.of());
        }
        List<ModelAuthorization> models = list.stream()
                .filter(item -> item instanceof Map)
                .map(item -> (Map<String, Object>) item)
                // 模型名称或有效期缺失时跳过该模型，避免错误放行。
                .map(m -> {
                    String name = stringValue(m.get("name"));
                    String expireAtStr = stringValue(m.get("expire_at"));
                    if (name == null || name.isBlank() || expireAtStr == null || expireAtStr.isBlank()) {
                        return null;
                    }
                    try {
                        return new ModelAuthorization(name, Instant.parse(expireAtStr));
                    } catch (DateTimeParseException e) {
                        log.warn("Invalid expire_at '{}' for model '{}', token hash={}", expireAtStr, name, token.hashCode());
                        return null;
                    }
                })
                .filter(java.util.Objects::nonNull)
                .toList();

        return new AuthResult(personId, tokenExpireAt, models);
    }

    /** 安全地把认证平台字段转为字符串 */
    private static String stringValue(Object value) {
        return value instanceof String s ? s : null;
    }
}
