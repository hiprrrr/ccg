package com.padb.ccg.routing;

import com.padb.ccg.core.model.ProviderConfig;
import com.padb.ccg.proxy.BedrockProperties;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class ProviderRegistryImplTest {

    private ProviderRegistryImpl registry;

    @BeforeEach
    void setUp() {
        var props = new BedrockProperties("us-east-1", null, null, 3, 120,
                List.of(
                        new ProviderConfig("m-1", "claude-opus-4-7", "us.anthropic.claude-opus-4-7-v1:0",
                                "us-west-2", List.of("text")),
                        new ProviderConfig("m-2", "claude-sonnet-4-6", "us.anthropic.claude-sonnet-4-6-v1:0",
                                "us-east-1", List.of("text", "vision"))
                ));
        registry = new ProviderRegistryImpl(props);
        registry.init();
    }

    @Test
    void shouldResolveKnownModel() {
        var result = registry.resolve("claude-opus-4-7");
        assertTrue(result.isPresent());
        assertEquals("us.anthropic.claude-opus-4-7-v1:0", result.get().bedrockModelId());
        assertEquals("us-west-2", result.get().region());
    }

    @Test
    void shouldReturnEmptyForUnknownModel() {
        var result = registry.resolve("nonexistent-model");
        assertTrue(result.isEmpty());
    }

    @Test
    void shouldRebuildCorrectly() {
        registry.rebuild(List.of(
                new ProviderConfig("m-3", "claude-haiku-4-5", "us.anthropic.claude-haiku-4-5-v1:0",
                        "us-west-2", List.of("text"))
        ));

        assertTrue(registry.resolve("claude-opus-4-7").isEmpty());
        assertTrue(registry.resolve("claude-haiku-4-5").isPresent());
        assertEquals(1, registry.modelCount());
    }

    @Test
    void shouldHandleDuplicateModelNames() {
        registry.rebuild(List.of(
                new ProviderConfig("m-a", "claude-opus-4-7", "id-first", "us-east-1", List.of()),
                new ProviderConfig("m-b", "claude-opus-4-7", "id-second", "us-west-2", List.of())
        ));

        var result = registry.resolve("claude-opus-4-7");
        assertTrue(result.isPresent());
        assertEquals("id-second", result.get().bedrockModelId());
    }

    @Test
    void shouldHandleEmptyRebuild() {
        registry.rebuild(List.of());
        assertEquals(0, registry.modelCount());
        assertTrue(registry.resolve("claude-opus-4-7").isEmpty());
    }
}
