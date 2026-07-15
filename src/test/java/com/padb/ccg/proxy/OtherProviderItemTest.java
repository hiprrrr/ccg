package com.padb.ccg.proxy;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class OtherProviderItemTest {

    @Test
    void shouldDefaultToPassthroughWhenApiFormatOmitted() {
        var item = new OtherProviderItem("basicrouter", "https://example.com/api", List.of("key"), null);
        assertEquals(UpstreamApiFormat.PASSTHROUGH, item.resolvedApiFormat());
        assertTrue(item.isPassthroughFormat());
        assertTrue(item.supportsOpenAiClientPassthrough());
        assertFalse(item.isOpenAiFormat());
        assertFalse(item.isAnthropicFormat());
    }

    @Test
    void shouldResolveAnthropicFormat() {
        var item = new OtherProviderItem("huawei", "https://example.com/anthropic", List.of("key"), "anthropic");
        assertEquals(UpstreamApiFormat.ANTHROPIC, item.resolvedApiFormat());
        assertTrue(item.isAnthropicFormat());
        assertFalse(item.supportsOpenAiClientPassthrough());
    }

    @Test
    void shouldResolveOpenAiFormat() {
        var item = new OtherProviderItem("huawei", "https://example.com/openai/v1", List.of("key"), "openai");
        assertTrue(item.isOpenAiFormat());
        assertTrue(item.supportsOpenAiClientPassthrough());
        assertEquals("https://example.com/openai/v1", item.resolvedBaseUrl());
    }

    @Test
    void shouldTrimTrailingSlashFromBaseUrl() {
        var item = new OtherProviderItem("huawei", "https://custom.example/openai/v1/", List.of("key"), "openai");
        assertEquals("https://custom.example/openai/v1", item.resolvedBaseUrl());
    }

    @Test
    void shouldRequireBaseUrl() {
        var item = new OtherProviderItem("huawei", null, List.of("key"), "anthropic");
        assertThrows(IllegalStateException.class, item::resolvedBaseUrl);
    }

    @Test
    void shouldFilterBlankApiKeys() {
        var item = new OtherProviderItem("huawei", "https://example.com", List.of("a", "", "b"), "anthropic");
        assertEquals(List.of("a", "b"), item.eligibleApiKeys());
    }
}
