package com.padb.ccg.auth;

import com.padb.ccg.core.exception.ProviderException;
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

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.mockito.ArgumentMatchers.anyString;
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
        var auths = List.of(new ModelAuthorization("claude-opus", Instant.now().plusSeconds(7200)));
        when(platformClient.fetchAuthorizations("user1")).thenReturn(Mono.just(auths));

        // First call fills cache
        cacheManager.getAuthorizations("user1").as(StepVerifier::create).expectNext(auths).verifyComplete();

        // Second call should hit cache
        cacheManager.getAuthorizations("user1").as(StepVerifier::create).expectNext(auths).verifyComplete();

        // Platform was called only once
        verify(platformClient, times(1)).fetchAuthorizations("user1");
    }

    @Test
    void shouldSingleflightConcurrentRequests() {
        var auths = List.of(new ModelAuthorization("claude-opus", Instant.now().plusSeconds(7200)));
        when(platformClient.fetchAuthorizations("user1")).thenReturn(Mono.just(auths));

        // Three concurrent subscribers share one platform call
        var r1 = cacheManager.getAuthorizations("user1");
        var r2 = cacheManager.getAuthorizations("user1");
        var r3 = cacheManager.getAuthorizations("user1");

        StepVerifier.create(Mono.zip(r1, r2, r3))
                .assertNext(t -> {
                    assert t.getT1() == auths;
                    assert t.getT2() == auths;
                    assert t.getT3() == auths;
                })
                .verifyComplete();

        verify(platformClient, times(1)).fetchAuthorizations("user1");
    }

    @Test
    void shouldFallbackToStaleWhenPlatformFailsAndCacheExpired() {
        var fresh = List.of(new ModelAuthorization("claude-opus", Instant.now().plusSeconds(100)));
        when(platformClient.fetchAuthorizations("user1"))
                .thenReturn(Mono.just(fresh))       // first call succeeds
                .thenReturn(Mono.error(new RuntimeException("platform down"))); // second fails

        // Use short TTL so cache expires quickly
        when(props.cacheTtlSeconds()).thenReturn(0); // 0 means expiry check filters to defaultTtl=0
        // Re-create manager with short TTL
        cacheManager = new AuthCacheManager(platformClient, props);

        // First call fills cache (0s TTL means immediate expiry)
        cacheManager.getAuthorizations("user1").as(StepVerifier::create)
                .expectNext(fresh).verifyComplete();

        // Need to wait a tiny bit for the nanos-based TTL to pass
        try { Thread.sleep(10); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }

        // Second call: cache miss (expired), platform fails → stale
        StepVerifier.create(cacheManager.getAuthorizations("user1"))
                .expectNext(fresh)
                .verifyComplete();
    }

    @Test
    void shouldThrowWhenNoStaleAndPlatformFails() {
        when(platformClient.fetchAuthorizations("user1"))
                .thenReturn(Mono.error(new RuntimeException("platform down")));

        StepVerifier.create(cacheManager.getAuthorizations("user1"))
                .expectErrorMatches(e -> e instanceof ProviderException
                        && e.getMessage().contains("Unable to verify authorization"))
                .verify();
    }

    @Test
    void shouldCleanInflightOnCancel() {
        when(platformClient.fetchAuthorizations("user1"))
                .thenReturn(Mono.never());

        var result = cacheManager.getAuthorizations("user1");
        result.subscribe().dispose();

        // After cancel, a new fetch should proceed (not stuck on orphaned inflight)
        var fresh = List.of(new ModelAuthorization("claude-opus", Instant.now().plusSeconds(7200)));
        when(platformClient.fetchAuthorizations("user1")).thenReturn(Mono.just(fresh));

        StepVerifier.create(cacheManager.getAuthorizations("user1"))
                .expectNext(fresh)
                .verifyComplete();
    }
}
