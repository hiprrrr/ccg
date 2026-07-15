package com.padb.ccg.routing;

import com.padb.ccg.core.model.ProviderConfig;
import com.padb.ccg.core.model.ProviderNames;
import com.padb.ccg.proxy.ModelMappingsProperties;
import com.padb.ccg.proxy.OtherProviderItem;
import com.padb.ccg.proxy.OtherProvidersProperties;
import com.padb.ccg.proxy.OtherProvidersRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class ProviderRegistryImplTest {

    private ProviderRegistryImpl registry;

    @BeforeEach
    void setUp() {
        var otherProps = new OtherProvidersProperties();
        otherProps.add(new OtherProviderItem(
                "huawei", "https://example.com/anthropic", List.of("key"), "anthropic"));
        var otherRegistry = new OtherProvidersRegistry(otherProps);
        otherRegistry.rebuild(otherProps);

        var props = new ModelMappingsProperties();
        props.add(new ModelMappingsProperties.ModelMappingItem(
                "claude-opus-4-7", ProviderNames.AWS,
                "us.anthropic.claude-opus-4-7-v1:0", "us-west-2", List.of("text")));
        props.add(new ModelMappingsProperties.ModelMappingItem(
                "claude-sonnet-4-6", ProviderNames.AWS,
                "us.anthropic.claude-sonnet-4-6-v1:0", "us-east-1", List.of("text", "vision")));
        props.add(new ModelMappingsProperties.ModelMappingItem(
                "deepseek-huawei", "huawei",
                "deepseek-v3.2", null, List.of("text", "stream")));
        registry = new ProviderRegistryImpl(props, otherRegistry);
        registry.init();
    }

    @Test
    void shouldResolveKnownAwsModel() {
        var result = registry.resolve("claude-opus-4-7");
        assertTrue(result.isPresent());
        assertEquals(ProviderNames.AWS, result.get().provider());
        assertTrue(result.get().isAws());
        assertEquals("us.anthropic.claude-opus-4-7-v1:0", result.get().upstreamModelId());
        assertEquals("us-west-2", result.get().region());
    }

    @Test
    void shouldResolveKnownOtherProviderModel() {
        var result = registry.resolve("deepseek-huawei");
        assertTrue(result.isPresent());
        assertEquals("huawei", result.get().provider());
        assertFalse(result.get().isAws());
        assertEquals("deepseek-v3.2", result.get().upstreamModelId());
    }

    @Test
    void shouldReturnEmptyForUnknownModel() {
        var result = registry.resolve("nonexistent-model");
        assertTrue(result.isEmpty());
    }

    @Test
    void shouldRebuildCorrectly() {
        registry.rebuild(List.of(
                new ProviderConfig(ProviderNames.AWS, "claude-haiku-4-5",
                        "us.anthropic.claude-haiku-4-5-v1:0", "us-west-2", List.of("text"))
        ));

        assertTrue(registry.resolve("claude-opus-4-7").isEmpty());
        assertTrue(registry.resolve("claude-haiku-4-5").isPresent());
        assertEquals(1, registry.modelCount());
    }

    @Test
    void shouldHandleDuplicateModelNames() {
        registry.rebuild(List.of(
                new ProviderConfig(ProviderNames.AWS, "claude-opus-4-7", "id-first", "us-east-1", List.of()),
                new ProviderConfig("huawei", "claude-opus-4-7", "id-second", null, List.of())
        ));

        var result = registry.resolve("claude-opus-4-7");
        assertTrue(result.isPresent());
        assertEquals("huawei", result.get().provider());
        assertEquals("id-second", result.get().upstreamModelId());
    }

    @Test
    void shouldHandleEmptyRebuild() {
        registry.rebuild(List.of());
        assertEquals(0, registry.modelCount());
        assertTrue(registry.resolve("claude-opus-4-7").isEmpty());
    }

    @Test
    void shouldRejectUnknownOtherProviderReference() {
        assertThrows(IllegalStateException.class, () ->
                registry.rebuild(List.of(
                        new ProviderConfig("unknown-vendor", "m1", "u1", null, List.of())
                )));
    }
}
