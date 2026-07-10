package com.padb.ccg.proxy;

import com.padb.ccg.core.exception.ProviderException;
import com.padb.ccg.core.model.ProviderAccount;
import com.padb.ccg.core.model.ProviderChannel;
import com.padb.ccg.core.model.ProviderConfig;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.*;

class UpstreamAccountSelectorTest {

    private static final HuaweiMaasProperties HUAWEI_GLOBAL =
            new HuaweiMaasProperties(null, "global-huawei-key", "openai", 3, 300);

    @Test
    void shouldPickHuaweiAccountFromPool() {
        var mapping = new ProviderConfig(
                ProviderChannel.HUAWEI, "glm-5.2", "glm-5.2", null, List.of(),
                List.of(
                        new ProviderAccount("a1", "key-1"),
                        new ProviderAccount("a2", "key-2"),
                        new ProviderAccount("a3", "key-3")
                ));

        Set<String> picked = IntStream.range(0, 30)
                .mapToObj(i -> UpstreamAccountSelector.selectHuawei(mapping, HUAWEI_GLOBAL).apiKey())
                .collect(Collectors.toSet());

        assertTrue(picked.contains("key-1"));
        assertTrue(picked.contains("key-2"));
        assertTrue(picked.contains("key-3"));
        assertFalse(picked.contains("global-huawei-key"));
    }

    @Test
    void shouldFallbackToGlobalHuaweiKeyWhenNoAccounts() {
        var mapping = new ProviderConfig(ProviderChannel.HUAWEI, "glm-5.2", "glm-5.2", null, List.of());

        var selection = UpstreamAccountSelector.selectHuawei(mapping, HUAWEI_GLOBAL);

        assertEquals("default", selection.accountId());
        assertEquals("global-huawei-key", selection.apiKey());
    }

    @Test
    void shouldSkipEmptyHuaweiAccountsButKeepValidOnes() {
        var mapping = new ProviderConfig(
                ProviderChannel.HUAWEI, "glm-5.2", "glm-5.2", null, List.of(),
                List.of(
                        new ProviderAccount("huawei-1", ""),
                        new ProviderAccount("huawei-2", "key-2"),
                        new ProviderAccount("huawei-3", "key-3")
                ));

        Set<String> picked = IntStream.range(0, 50)
                .mapToObj(i -> UpstreamAccountSelector.selectHuawei(mapping, HUAWEI_GLOBAL).apiKey())
                .collect(Collectors.toSet());

        assertFalse(picked.contains("key-1"));
        assertTrue(picked.contains("key-2"));
        assertTrue(picked.contains("key-3"));
        assertFalse(picked.contains("global-huawei-key"));
    }

    @Test
    void shouldDistributeHuaweiAccountsRoughlyEvenly() {
        var mapping = new ProviderConfig(
                ProviderChannel.HUAWEI, "glm-5.2", "glm-5.2", null, List.of(),
                List.of(
                        new ProviderAccount("a1", "key-1"),
                        new ProviderAccount("a2", "key-2"),
                        new ProviderAccount("a3", "key-3")
                ));

        long firstAccountHits = IntStream.range(0, 3000)
                .mapToObj(i -> UpstreamAccountSelector.selectHuawei(mapping, HUAWEI_GLOBAL).accountId())
                .filter("a1"::equals)
                .count();

        // 3000 次均匀随机，首个账号期望约 1000 次；放宽到 700~1300 避免偶发失败
        assertTrue(firstAccountHits >= 700 && firstAccountHits <= 1300,
                "first account hit count=" + firstAccountHits + ", expected ~1000");
    }

    @Test
    void shouldSkipInvalidHuaweiAccountsAndUseGlobal() {
        var mapping = new ProviderConfig(
                ProviderChannel.HUAWEI, "glm-5.2", "glm-5.2", null, List.of(),
                List.of(new ProviderAccount("empty", "")));

        var selection = UpstreamAccountSelector.selectHuawei(mapping, HUAWEI_GLOBAL);

        assertEquals("default", selection.accountId());
        assertEquals("global-huawei-key", selection.apiKey());
    }

    @Test
    void shouldFailWhenNoHuaweiKeyAvailable() {
        var mapping = new ProviderConfig(ProviderChannel.HUAWEI, "glm-5.2", "glm-5.2", null, List.of());
        var emptyGlobal = new HuaweiMaasProperties(null, "", "openai", 3, 300);

        assertThrows(ProviderException.class,
                () -> UpstreamAccountSelector.selectHuawei(mapping, emptyGlobal));
    }
}
