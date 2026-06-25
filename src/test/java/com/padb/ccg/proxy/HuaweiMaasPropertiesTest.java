package com.padb.ccg.proxy;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class HuaweiMaasPropertiesTest {

    @Test
    void shouldDefaultToAnthropicBaseUrl() {
        var props = new HuaweiMaasProperties(null, "key", null, 3, 300);
        assertEquals(HuaweiMaasApiFormat.ANTHROPIC, props.resolvedApiFormat());
        assertTrue(props.resolvedBaseUrl().endsWith("/anthropic"));
    }

    @Test
    void shouldUseOpenAiDefaultBaseUrlWhenFormatIsOpenAi() {
        var props = new HuaweiMaasProperties(null, "key", "openai", 3, 300);
        assertTrue(props.isOpenAiFormat());
        assertTrue(props.resolvedBaseUrl().endsWith("/openai/v1"));
    }

    @Test
    void shouldPreferExplicitBaseUrl() {
        var props = new HuaweiMaasProperties("https://custom.example/openai/v1", "key", "openai", 3, 300);
        assertEquals("https://custom.example/openai/v1", props.resolvedBaseUrl());
    }
}
