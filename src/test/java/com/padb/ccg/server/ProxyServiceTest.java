package com.padb.ccg.server;

import com.padb.ccg.auth.AuthService;
import com.padb.ccg.core.model.ProviderConfig;
import com.padb.ccg.core.spi.RateLimiter;
import com.padb.ccg.core.spi.RequestLogger;
import com.padb.ccg.proxy.BedrockProxyHandler;
import com.padb.ccg.proxy.ProxyService;
import com.padb.ccg.routing.ProviderRegistryImpl;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.reactive.WebFluxTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Import;
import org.springframework.test.web.reactive.server.WebTestClient;
import reactor.core.publisher.Mono;

import java.util.Optional;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

@WebFluxTest
@Import({GatewayRouter.class, GatewayHandler.class, ProxyService.class})
class ProxyServiceTest {

    @Autowired
    private WebTestClient webClient;

    @MockBean
    private AuthService authService;

    @MockBean
    private RateLimiter rateLimiter;

    @MockBean
    private ProviderRegistryImpl providerRegistry;

    @MockBean
    private BedrockProxyHandler bedrockHandler;

    @MockBean
    private RequestLogger requestLogger;

    @Test
    void shouldReturn403WhenMissingApiKey() {
        webClient.post()
                .uri("/v1/messages")
                .header("Content-Type", "application/json")
                .bodyValue("{\"model\":\"claude-opus-4-7\"}")
                .exchange()
                .expectStatus().isForbidden()
                .expectBody()
                .jsonPath("$.error.type").isEqualTo("permission_error");
    }

    @Test
    void shouldReturn400WhenMissingModel() {
        webClient.post()
                .uri("/v1/messages")
                .header("x-api-key", "user1")
                .header("Content-Type", "application/json")
                .bodyValue("{\"messages\":[]}")
                .exchange()
                .expectStatus().isBadRequest()
                .expectBody()
                .jsonPath("$.error.type").isEqualTo("invalid_request_error");
    }

    @Test
    void shouldReturn502WhenModelNotMapped() {
        when(providerRegistry.resolve(anyString())).thenReturn(Optional.empty());

        webClient.post()
                .uri("/v1/messages")
                .header("x-api-key", "user1")
                .header("Content-Type", "application/json")
                .bodyValue("{\"model\":\"unknown-model\"}")
                .exchange()
                .expectStatus().isEqualTo(502)
                .expectBody()
                .jsonPath("$.error.type").isEqualTo("api_error");
    }

    @Test
    void shouldReturn403WhenUnauthorized() {
        var mapping = new ProviderConfig("m-1", "claude-opus-4-7",
                "us.anthropic.claude-opus-4-7-v1:0", "us-west-2", java.util.List.of("text"));
        when(providerRegistry.resolve("claude-opus-4-7")).thenReturn(Optional.of(mapping));
        when(authService.authorize("user1", "claude-opus-4-7"))
                .thenReturn(Mono.error(new com.padb.ccg.core.exception.UnauthorizedException("not authorized")));

        webClient.post()
                .uri("/v1/messages")
                .header("x-api-key", "user1")
                .header("Content-Type", "application/json")
                .bodyValue("{\"model\":\"claude-opus-4-7\"}")
                .exchange()
                .expectStatus().isForbidden();
    }

    @Test
    void shouldAcceptBearerTokenAsUsername() {
        var mapping = new ProviderConfig("m-1", "claude-opus-4-7",
                "us.anthropic.claude-opus-4-7-v1:0", "us-west-2", java.util.List.of("text"));
        when(providerRegistry.resolve("claude-opus-4-7")).thenReturn(Optional.of(mapping));
        when(authService.authorize("user1", "claude-opus-4-7")).thenReturn(Mono.empty());
        when(rateLimiter.tryAcquire("user1")).thenReturn(Mono.just(true));
        when(bedrockHandler.forward(eq(mapping), anyString(), any(), any(), eq("user1"), eq("claude-opus-4-7"), anyString()))
                .thenReturn(reactor.core.publisher.Flux.empty());

        webClient.post()
                .uri("/v1/messages")
                .header("Authorization", "Bearer user1")
                .header("Content-Type", "application/json")
                .bodyValue("{\"model\":\"claude-opus-4-7\"}")
                .exchange()
                .expectStatus().isOk();
    }

    @Test
    void shouldReturn200ForHealth() {
        webClient.get()
                .uri("/health")
                .exchange()
                .expectStatus().isOk();
    }
}
