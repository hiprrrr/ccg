package com.padb.ccg.server;

import com.padb.ccg.auth.AuthService;
import com.padb.ccg.core.model.AuthResult;
import com.padb.ccg.core.model.ModelAuthorization;
import com.padb.ccg.core.model.ProviderNames;
import com.padb.ccg.core.model.ProviderConfig;
import com.padb.ccg.core.spi.RateLimiter;
import com.padb.ccg.core.spi.RequestLogger;
import com.padb.ccg.proxy.LlmUpstreamRouter;
import com.padb.ccg.proxy.OpenAiProxyService;
import com.padb.ccg.proxy.ProxyRequestSupport;
import com.padb.ccg.proxy.ProxyService;
import com.padb.ccg.routing.ProviderRegistryImpl;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.reactive.WebFluxTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Import;
import org.springframework.test.web.reactive.server.WebTestClient;
import org.springframework.web.reactive.function.server.ServerRequest;
import org.springframework.web.reactive.function.server.ServerResponse;
import reactor.core.publisher.Mono;

import java.util.Optional;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

@WebFluxTest
@Import({GatewayRouter.class, GatewayHandler.class, ProxyService.class, ProxyRequestSupport.class})
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
    private LlmUpstreamRouter upstreamRouter;

    @MockBean
    private OpenAiProxyService openAiProxyService;

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
        var mapping = new ProviderConfig(ProviderNames.AWS, "claude-opus-4-7",
                "us.anthropic.claude-opus-4-7-v1:0", "us-west-2", java.util.List.of("text"));
        when(providerRegistry.resolve("claude-opus-4-7")).thenReturn(Optional.of(mapping));
        when(authService.authorize("token1", "claude-opus-4-7"))
                .thenReturn(Mono.error(new com.padb.ccg.core.exception.UnauthorizedException("not authorized")));

        webClient.post()
                .uri("/v1/messages")
                .header("x-api-key", "token1")
                .header("Content-Type", "application/json")
                .bodyValue("{\"model\":\"claude-opus-4-7\"}")
                .exchange()
                .expectStatus().isForbidden();
    }

    @Test
    void shouldAcceptBearerTokenAndUsePersonId() {
        var mapping = new ProviderConfig(ProviderNames.AWS, "claude-opus-4-7",
                "us.anthropic.claude-opus-4-7-v1:0", "us-west-2", java.util.List.of("text"));
        var authResult = new AuthResult("person1", java.time.Instant.now().plusSeconds(7200),
                java.util.List.of(new ModelAuthorization("claude-opus-4-7", java.time.Instant.now().plusSeconds(7200))));
        when(providerRegistry.resolve("claude-opus-4-7")).thenReturn(Optional.of(mapping));
        when(authService.authorize("token1", "claude-opus-4-7")).thenReturn(Mono.just(authResult));
        when(rateLimiter.tryAcquire("person1")).thenReturn(Mono.just(true));
        when(upstreamRouter.forward(eq(mapping), anyString(), any(), any(), eq("person1"), eq("claude-opus-4-7"), anyString()))
                .thenReturn(reactor.core.publisher.Flux.empty());

        webClient.post()
                .uri("/v1/messages")
                .header("Authorization", "Bearer token1")
                .header("Content-Type", "application/json")
                .bodyValue("{\"model\":\"claude-opus-4-7\"}")
                .exchange()
                .expectStatus().isOk();
    }

    @Test
    void shouldRouteChatCompletionsWithoutV1Prefix() {
        when(openAiProxyService.process(any(ServerRequest.class)))
                .thenReturn(ServerResponse.ok().build());

        webClient.post()
                .uri("/chat/completions")
                .header("x-api-key", "token1")
                .header("Content-Type", "application/json")
                .bodyValue("{\"model\":\"glm-5\",\"messages\":[]}")
                .exchange()
                .expectStatus().isOk();
    }

    @Test
    void shouldRouteResponsesWithoutV1Prefix() {
        when(openAiProxyService.processResponses(any(ServerRequest.class)))
                .thenReturn(ServerResponse.ok().build());

        webClient.post()
                .uri("/responses")
                .header("x-api-key", "token1")
                .header("Content-Type", "application/json")
                .bodyValue("{\"model\":\"glm-5\",\"input\":\"hi\"}")
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
