package com.padb.ccg.auth;

import com.padb.ccg.core.model.AuthResult;
import com.padb.ccg.core.model.ModelAuthorization;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.Instant;
import java.util.List;

import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class AuthCacheManagerTest {

    @Mock
    private AuthPlatformClientImpl platformClient;

    @Mock
    private AuthProperties props;

    private AuthCacheManager cacheManager;

    @BeforeEach
    void setUp() {
        when(props.cacheTtlSeconds()).thenReturn(3600);
        cacheManager = new AuthCacheManager(platformClient, props);
    }

    @Test
    void shouldReturnCachedDataOnHit() {
        var authResult = new AuthResult("person1", Instant.now().plusSeconds(7200),
                List.of(new ModelAuthorization("claude-opus", Instant.now().plusSeconds(7200))));
        when(platformClient.fetchAuthorization("token1")).thenReturn(Mono.just(authResult));

        // First call fills cache
        cacheManager.getAuthorization("token1", "claude-opus").as(StepVerifier::create).expectNext(authResult).verifyComplete();

        // Second call should hit cache
        cacheManager.getAuthorization("token1", "claude-opus").as(StepVerifier::create).expectNext(authResult).verifyComplete();

        // Platform was called only once
        verify(platformClient, times(1)).fetchAuthorization("token1");
    }

    @Test
    void shouldSingleflightConcurrentRequests() {
        var authResult = new AuthResult("person1", Instant.now().plusSeconds(7200),
                List.of(new ModelAuthorization("claude-opus", Instant.now().plusSeconds(7200))));
        when(platformClient.fetchAuthorization("token1")).thenReturn(Mono.just(authResult));

        // Three concurrent subscribers share one platform call
        var r1 = cacheManager.getAuthorization("token1", "claude-opus");
        var r2 = cacheManager.getAuthorization("token1", "claude-opus");
        var r3 = cacheManager.getAuthorization("token1", "claude-opus");

        StepVerifier.create(Mono.zip(r1, r2, r3))
                .assertNext(t -> {
                    assert t.getT1() == authResult;
                    assert t.getT2() == authResult;
                    assert t.getT3() == authResult;
                })
                .verifyComplete();

        verify(platformClient, times(1)).fetchAuthorization("token1");
    }

    @Test
    void shouldRefetchWhenCachedModelIsMissing() {
        var first = new AuthResult("person1", Instant.now().plusSeconds(7200),
                List.of(new ModelAuthorization("claude-sonnet", Instant.now().plusSeconds(7200))));
        var refreshed = new AuthResult("person1", Instant.now().plusSeconds(7200),
                List.of(new ModelAuthorization("claude-opus", Instant.now().plusSeconds(7200))));
        when(platformClient.fetchAuthorization("token1"))
                .thenReturn(Mono.just(first))
                .thenReturn(Mono.just(refreshed));

        cacheManager.getAuthorization("token1", "claude-sonnet").as(StepVerifier::create)
                .expectNext(first).verifyComplete();

        StepVerifier.create(cacheManager.getAuthorization("token1", "claude-opus"))
                .expectNext(refreshed)
                .verifyComplete();

        verify(platformClient, times(2)).fetchAuthorization("token1");
    }

    @Test
    void shouldThrowWhenNoStaleAndPlatformFails() {
        when(platformClient.fetchAuthorization("token1"))
                .thenReturn(Mono.error(new RuntimeException("platform down")));

        StepVerifier.create(cacheManager.getAuthorization("token1", "claude-opus"))
                .expectErrorMatches(e -> e instanceof com.padb.ccg.core.exception.AuthPlatformUnavailableException
                        && e.getMessage().contains("Unable to verify authorization"))
                .verify();
    }

    @Test
    void shouldCleanInflightOnCancel() {
        when(platformClient.fetchAuthorization("token1"))
                .thenReturn(Mono.never());

        var result = cacheManager.getAuthorization("token1", "claude-opus");
        result.subscribe().dispose();

        // After cancel, a new fetch should proceed (not stuck on orphaned inflight)
        var fresh = new AuthResult("person1", Instant.now().plusSeconds(7200),
                List.of(new ModelAuthorization("claude-opus", Instant.now().plusSeconds(7200))));
        when(platformClient.fetchAuthorization("token1")).thenReturn(Mono.just(fresh));

        StepVerifier.create(cacheManager.getAuthorization("token1", "claude-opus"))
                .expectNext(fresh)
                .verifyComplete();
    }

    @Test
    void shouldNotPoisonSharedFetchWhenOneSubscriberCancels() {
        var authResult = new AuthResult("person1", Instant.now().plusSeconds(7200),
                List.of(new ModelAuthorization("claude-opus", Instant.now().plusSeconds(7200))));
        // 平台调用不立即完成，保证两个订阅者都挂在同一个 inflight 上
        var pending = reactor.core.publisher.Sinks.<AuthResult>one();
        when(platformClient.fetchAuthorization("token1")).thenReturn(pending.asMono());

        var first = cacheManager.getAuthorization("token1", "claude-opus");
        var second = cacheManager.getAuthorization("token1", "claude-opus");

        // 第一个订阅者取消：不得取消共享 Future
        first.subscribe().dispose();

        // 第二个订阅者仍能拿到平台调用结果
        var verifier = StepVerifier.create(second).then(() -> pending.tryEmitValue(authResult))
                .expectNext(authResult).verifyComplete();

        verify(platformClient, times(1)).fetchAuthorization("token1");
    }
}
