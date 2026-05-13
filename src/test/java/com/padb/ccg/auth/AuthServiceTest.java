package com.padb.ccg.auth;

import com.padb.ccg.core.exception.UnauthorizedException;
import com.padb.ccg.core.model.AuthResult;
import com.padb.ccg.core.model.ModelAuthorization;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.Instant;
import java.util.List;

import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class AuthServiceTest {

    @Mock
    private AuthCacheManager cacheManager;

    private AuthService authService;

    @BeforeEach
    void setUp() {
        var props = new AuthProperties("http://localhost:8081", 5, 28800, false);
        authService = new AuthService(cacheManager, props);
    }

    @Test
    void shouldAuthorizeValidModel() {
        var auths = List.of(
                new ModelAuthorization("claude-opus-4-7", Instant.now().plusSeconds(7200)),
                new ModelAuthorization("claude-sonnet-4-6", Instant.now().plusSeconds(3600))
        );
        var authResult = new AuthResult("person1", Instant.now().plusSeconds(7200), auths);
        when(cacheManager.getAuthorization("token1", "claude-opus-4-7")).thenReturn(Mono.just(authResult));

        var result = authService.authorize("token1", "claude-opus-4-7");
        StepVerifier.create(result).expectNext(authResult).verifyComplete();
    }

    @Test
    void shouldRejectUnauthorizedModel() {
        var auths = List.of(
                new ModelAuthorization("claude-sonnet-4-6", Instant.now().plusSeconds(3600))
        );
        when(cacheManager.getAuthorization("token1", "claude-opus-4-7"))
                .thenReturn(Mono.just(new AuthResult("person1", Instant.now().plusSeconds(7200), auths)));

        var result = authService.authorize("token1", "claude-opus-4-7");
        StepVerifier.create(result)
                .expectErrorMatches(e -> e instanceof UnauthorizedException
                        && e.getMessage().contains("not authorized"))
                .verify();
    }

    @Test
    void shouldRejectExpiredAuthorization() {
        var auths = List.of(
                new ModelAuthorization("claude-opus-4-7", Instant.now().minusSeconds(60))
        );
        when(cacheManager.getAuthorization("token1", "claude-opus-4-7"))
                .thenReturn(Mono.just(new AuthResult("person1", Instant.now().plusSeconds(7200), auths)));

        var result = authService.authorize("token1", "claude-opus-4-7");
        StepVerifier.create(result)
                .expectErrorMatches(e -> e instanceof UnauthorizedException
                        && e.getMessage().contains("expired"))
                .verify();
    }

    @Test
    void shouldRejectEmptyAuthorizationList() {
        when(cacheManager.getAuthorization("token1", "claude-opus-4-7"))
                .thenReturn(Mono.just(new AuthResult("person1", Instant.now().plusSeconds(7200), List.of())));

        var result = authService.authorize("token1", "claude-opus-4-7");
        StepVerifier.create(result)
                .expectErrorMatches(e -> e instanceof UnauthorizedException)
                .verify();
    }

    @Test
    void shouldPropagateAuthPlatformError() {
        when(cacheManager.getAuthorization("token1", "claude-opus-4-7"))
                .thenReturn(Mono.error(new RuntimeException("platform unreachable")));

        var result = authService.authorize("token1", "claude-opus-4-7");
        StepVerifier.create(result)
                .expectError(RuntimeException.class)
                .verify();
    }
}
