package com.padb.ccg.auth;

import com.padb.ccg.core.model.ModelAuthorization;
import com.padb.ccg.core.exception.ProviderException;
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
    public Mono<List<ModelAuthorization>> fetchAuthorizations(String username) {
        URI uri = URI.create(props.platformUrl() + "?username=" + encode(username));

        return webClient.get()
                .uri(uri)
                .retrieve()
                // 4xx 客户端错误 → 转为 ProviderException
                .onStatus(s -> s.is4xxClientError(), response -> {
                    log.warn("Auth platform returned client error {} for user={}", response.statusCode(), username);
                    return Mono.error(new ProviderException(
                            "Auth platform client error: " + response.statusCode()));
                })
                // 5xx 服务端错误 → 转为 ProviderException
                .onStatus(s -> s.is5xxServerError(), response -> {
                    log.warn("Auth platform returned server error {} for user={}", response.statusCode(), username);
                    return Mono.error(new ProviderException(
                            "Auth platform server error: " + response.statusCode()));
                })
                .bodyToMono(Map.class)
                .timeout(Duration.ofSeconds(props.platformTimeoutSeconds()))
                .map(body -> parseAuthorizations(body, username));
    }

    /** URL 编码工具方法 */
    private static String encode(String value) {
        return java.net.URLEncoder.encode(value, java.nio.charset.StandardCharsets.UTF_8);
    }

    /**
     * 解析认证平台返回的 JSON 响应，提取模型授权列表
     * 预期格式: {"models": [{"name": "...", "expire_at": "ISO-8601"}, ...]}
     */
    @SuppressWarnings("unchecked")
    private List<ModelAuthorization> parseAuthorizations(Map<?, ?> body, String username) {
        Object modelsObj = body.get("models");
        if (!(modelsObj instanceof List<?> list)) {
            log.warn("Auth platform returned unexpected format for user={}", username);
            return List.of();
        }
        return list.stream()
                .filter(item -> item instanceof Map)
                .map(item -> (Map<String, Object>) item)
                .map(m -> {
                    String name = (String) m.get("name");
                    // 未设置过期时间时默认为永不过期
                    Instant expireAt = Instant.MAX;
                    String expireAtStr = (String) m.get("expire_at");
                    if (expireAtStr != null) {
                        try {
                            expireAt = Instant.parse(expireAtStr);
                        } catch (DateTimeParseException e) {
                            log.warn("Invalid expire_at '{}' for model '{}', user={}", expireAtStr, name, username);
                        }
                    }
                    return new ModelAuthorization(name, expireAt);
                })
                .toList();
    }
}
