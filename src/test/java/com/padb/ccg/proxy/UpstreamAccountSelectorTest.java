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

    private static final BedrockProperties BEDROCK_GLOBAL =
            new BedrockProperties("us-east-1", "global-ak", "global-sk", null, 3, 300, "anthropic", false, 4096);

    @Test
    void shouldPickHuaweiAccountFromPool() {
        var mapping = new ProviderConfig(
                ProviderChannel.HUAWEI, "glm-5.2", "glm-5.2", null, List.of(),
                List.of(
                        new ProviderAccount("a1", "key-1", null, null, null, null),
                        new ProviderAccount("a2", "key-2", null, null, null, null),
                        new ProviderAccount("a3", "key-3", null, null, null, null)
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
    void shouldSkipInvalidHuaweiAccountsAndUseGlobal() {
        var mapping = new ProviderConfig(
                ProviderChannel.HUAWEI, "glm-5.2", "glm-5.2", null, List.of(),
                List.of(new ProviderAccount("empty", "", null, null, null, null)));

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

    @Test
    void shouldPickAwsAccountFromPool() {
        var mapping = new ProviderConfig(
                ProviderChannel.AWS, "glm-5", "zai.glm-5", "us-east-1", List.of(),
                List.of(
                        new ProviderAccount("aws-1", null, "ak-1", "sk-1", null, null),
                        new ProviderAccount("aws-2", null, "ak-2", "sk-2", null, null)
                ));

        Set<String> picked = IntStream.range(0, 20)
                .mapToObj(i -> UpstreamAccountSelector.selectAws(mapping, BEDROCK_GLOBAL).accessKey())
                .collect(Collectors.toSet());

        assertTrue(picked.contains("ak-1"));
        assertTrue(picked.contains("ak-2"));
        assertFalse(picked.contains("global-ak"));
    }

    @Test
    void shouldUseGlobalBedrockCredentialsWhenNoAccounts() {
        var mapping = new ProviderConfig(ProviderChannel.AWS, "glm-5", "zai.glm-5", "us-east-1", List.of());

        var selection = UpstreamAccountSelector.selectAws(mapping, BEDROCK_GLOBAL);

        assertEquals("default", selection.accountId());
        assertEquals("global-ak", selection.accessKey());
        assertEquals("global-sk", selection.secretKey());
        assertFalse(selection.useDefaultChain());
    }

    @Test
    void shouldUseDefaultCredentialChainWhenGlobalAwsKeyMissing() {
        var mapping = new ProviderConfig(ProviderChannel.AWS, "glm-5", "zai.glm-5", "us-east-1", List.of());
        var noKeyGlobal = new BedrockProperties("us-east-1", "", "", null, 3, 300, "anthropic", false, 4096);

        var selection = UpstreamAccountSelector.selectAws(mapping, noKeyGlobal);

        assertEquals("default", selection.accountId());
        assertTrue(selection.useDefaultChain());
    }

    @Test
    void shouldRespectPerAccountAwsRegionOverride() {
        var mapping = new ProviderConfig(
                ProviderChannel.AWS, "glm-5", "zai.glm-5", "us-east-1", List.of(),
                List.of(new ProviderAccount("west", null, "ak-west", "sk-west", null, "us-west-2")));

        var selection = UpstreamAccountSelector.selectAws(mapping, BEDROCK_GLOBAL);

        assertEquals("west", selection.accountId());
        assertEquals("us-west-2", selection.regionOverride());
    }
}
