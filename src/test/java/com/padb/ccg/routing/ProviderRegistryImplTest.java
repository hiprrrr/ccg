package com.padb.ccg.routing;

import com.padb.ccg.core.model.ProviderChannel;
import com.padb.ccg.core.model.ProviderConfig;
import com.padb.ccg.proxy.ModelMappingsProperties;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class ProviderRegistryImplTest {

    private ProviderRegistryImpl registry;

    @BeforeEach
    void setUp() {
        var props = new ModelMappingsProperties();
        props.add(new ModelMappingsProperties.ModelMappingItem(
                "claude-opus-4-7", ProviderChannel.AWS,
                "us.anthropic.claude-opus-4-7-v1:0", "us-west-2", List.of("text")));
        props.add(new ModelMappingsProperties.ModelMappingItem(
                "claude-sonnet-4-6", ProviderChannel.AWS,
                "us.anthropic.claude-sonnet-4-6-v1:0", "us-east-1", List.of("text", "vision")));
        props.add(new ModelMappingsProperties.ModelMappingItem(
                "deepseek-huawei", ProviderChannel.HUAWEI,
                "deepseek-v3.2", null, List.of("text", "stream")));
        registry = new ProviderRegistryImpl(props);
        registry.init();
    }

    @Test
    void shouldResolveKnownAwsModel() {
        var result = registry.resolve("claude-opus-4-7");
        assertTrue(result.isPresent());
        assertEquals(ProviderChannel.AWS, result.get().provider());
        assertEquals("us.anthropic.claude-opus-4-7-v1:0", result.get().upstreamModelId());
        assertEquals("us-west-2", result.get().region());
    }

    @Test
    void shouldResolveKnownHuaweiModel() {
        var result = registry.resolve("deepseek-huawei");
        assertTrue(result.isPresent());
        assertEquals(ProviderChannel.HUAWEI, result.get().provider());
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
                new ProviderConfig(ProviderChannel.AWS, "claude-haiku-4-5",
                        "us.anthropic.claude-haiku-4-5-v1:0", "us-west-2", List.of("text"))
        ));

        assertTrue(registry.resolve("claude-opus-4-7").isEmpty());
        assertTrue(registry.resolve("claude-haiku-4-5").isPresent());
        assertEquals(1, registry.modelCount());
    }

    @Test
    void shouldHandleDuplicateModelNames() {
        registry.rebuild(List.of(
                new ProviderConfig(ProviderChannel.AWS, "claude-opus-4-7", "id-first", "us-east-1", List.of()),
                new ProviderConfig(ProviderChannel.HUAWEI, "claude-opus-4-7", "id-second", null, List.of())
        ));

        var result = registry.resolve("claude-opus-4-7");
        assertTrue(result.isPresent());
        assertEquals(ProviderChannel.HUAWEI, result.get().provider());
        assertEquals("id-second", result.get().upstreamModelId());
    }

    @Test
    void shouldHandleEmptyRebuild() {
        registry.rebuild(List.of());
        assertEquals(0, registry.modelCount());
        assertTrue(registry.resolve("claude-opus-4-7").isEmpty());
    }
}
