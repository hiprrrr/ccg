package com.padb.ccg.proxy;

import com.padb.ccg.core.exception.ProviderException;
import com.padb.ccg.core.model.ProviderNames;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.*;

class UpstreamAccountSelectorTest {

    @Test
    void shouldPickKeyFromPool() {
        var provider = new OtherProviderItem(
                "huawei", "https://example.com/anthropic",
                List.of("key-1", "key-2", "key-3"), "openai");

        Set<String> picked = IntStream.range(0, 30)
                .mapToObj(i -> UpstreamAccountSelector.select(provider).apiKey())
                .collect(Collectors.toSet());

        assertTrue(picked.contains("key-1"));
        assertTrue(picked.contains("key-2"));
        assertTrue(picked.contains("key-3"));
    }

    @Test
    void shouldUseSingleKey() {
        var provider = new OtherProviderItem(
                "huawei", "https://example.com/anthropic",
                List.of("only-key"), "openai");

        var selection = UpstreamAccountSelector.select(provider);

        assertEquals("only-key", selection.apiKey());
        assertTrue(selection.accountId().startsWith("huawei-"));
    }

    @Test
    void shouldSkipBlankKeysButKeepValidOnes() {
        var provider = new OtherProviderItem(
                "huawei", "https://example.com/anthropic",
                List.of("", "key-2", "key-3"), "openai");

        Set<String> picked = IntStream.range(0, 50)
                .mapToObj(i -> UpstreamAccountSelector.select(provider).apiKey())
                .collect(Collectors.toSet());

        assertFalse(picked.contains(""));
        assertTrue(picked.contains("key-2"));
        assertTrue(picked.contains("key-3"));
    }

    @Test
    void shouldDistributeKeysRoughlyEvenly() {
        var provider = new OtherProviderItem(
                "huawei", "https://example.com/anthropic",
                List.of("key-1", "key-2", "key-3"), "openai");

        long firstKeyHits = IntStream.range(0, 3000)
                .mapToObj(i -> UpstreamAccountSelector.select(provider).apiKey())
                .filter("key-1"::equals)
                .count();

        assertTrue(firstKeyHits >= 700 && firstKeyHits <= 1300,
                "first key hit count=" + firstKeyHits + ", expected ~1000");
    }

    @Test
    void shouldFailWhenOnlyBlankKeys() {
        var provider = new OtherProviderItem(
                "huawei", "https://example.com/anthropic",
                List.of("", "  "), "openai");

        assertThrows(ProviderException.class, () -> UpstreamAccountSelector.select(provider));
    }

    @Test
    void shouldFailWhenNoKeys() {
        var empty = new OtherProviderItem("huawei", "https://example.com", List.of(), "openai");

        assertThrows(ProviderException.class, () -> UpstreamAccountSelector.select(empty));
    }

    @Test
    void shouldExcludeTriedKeys() {
        var provider = new OtherProviderItem(
                "huawei", "https://example.com/anthropic",
                List.of("key-1", "key-2", "key-3"), "openai");

        // 排除两个 key 后，必须命中剩余的那个
        Set<String> picked = IntStream.range(0, 30)
                .mapToObj(i -> UpstreamAccountSelector.select(provider, Set.of("key-1", "key-3")).apiKey())
                .collect(Collectors.toSet());

        assertEquals(Set.of("key-2"), picked);
    }

    @Test
    void shouldFailWhenAllKeysExcluded() {
        var provider = new OtherProviderItem(
                "huawei", "https://example.com/anthropic",
                List.of("key-1", "key-2"), "openai");

        assertThrows(ProviderException.class,
                () -> UpstreamAccountSelector.select(provider, Set.of("key-1", "key-2")));
    }

    @Test
    void awsProviderNameIsRecognized() {
        assertTrue(ProviderNames.isAws("aws"));
        assertTrue(ProviderNames.isAws("AWS"));
        assertFalse(ProviderNames.isAws("huawei"));
    }
}
