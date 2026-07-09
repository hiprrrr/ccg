package com.padb.ccg.proxy;

import com.padb.ccg.core.model.ProviderAccount;
import com.padb.ccg.core.model.ProviderChannel;
import com.padb.ccg.core.model.ProviderConfig;
import com.padb.ccg.core.exception.ProviderException;

import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

/**
 * 按模型映射从多账号池中随机选取一个上游账号；未配置 accounts 时回退到全局凭证。
 */
public final class UpstreamAccountSelector {

    private static final String DEFAULT_ACCOUNT_ID = "default";

    private UpstreamAccountSelector() {
    }

    /**
     * 华为 MaaS 账号选择结果。
     *
     * @param accountId 账号标识（日志用，不含密钥）
     * @param apiKey    实际上游 API Key
     */
    public record HuaweiSelection(String accountId, String apiKey) {
    }

    /**
     * AWS Bedrock 账号选择结果。
     *
     * @param accountId       账号标识
     * @param accessKey       静态 Access Key；为 null 表示走默认凭证链
     * @param secretKey       静态 Secret Key
     * @param sessionToken    STS 会话令牌（可选）
     * @param regionOverride  单账号区域覆盖（可选）
     * @param useDefaultChain 为 true 时使用 {@link software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider}
     */
    public record AwsSelection(String accountId, String accessKey, String secretKey, String sessionToken,
                                String regionOverride, boolean useDefaultChain) {
    }

    /**
     * 为华为 MaaS 随机选取一个账号；模型未配置 accounts 时使用 {@code huawei-maas.api-key}。
     */
    public static HuaweiSelection selectHuawei(ProviderConfig mapping, HuaweiMaasProperties global) {
        ProviderAccount picked = pickHuaweiAccount(mapping);
        if (picked != null) {
            return new HuaweiSelection(
                    picked.resolvedId("huawei-" + Integer.toHexString(picked.apiKey().hashCode())),
                    picked.apiKey());
        }
        String globalKey = global.apiKey();
        if (globalKey == null || globalKey.isBlank()) {
            throw new ProviderException("Huawei MaaS API key is not configured for model '" + mapping.modelName() + "'");
        }
        return new HuaweiSelection(DEFAULT_ACCOUNT_ID, globalKey);
    }

    /**
     * 为 AWS Bedrock 随机选取一个账号；模型未配置 accounts 时使用全局 {@code bedrock.*} 或默认凭证链。
     */
    public static AwsSelection selectAws(ProviderConfig mapping, BedrockProperties global) {
        ProviderAccount picked = pickAwsAccount(mapping);
        if (picked != null) {
            return new AwsSelection(
                    picked.resolvedId("aws-" + Integer.toHexString(picked.accessKey().hashCode())),
                    picked.accessKey(),
                    picked.secretKey(),
                    picked.sessionToken(),
                    blankToNull(picked.region()),
                    false);
        }
        String ak = global.accessKey();
        if (ak == null || ak.isBlank()) {
            return new AwsSelection(DEFAULT_ACCOUNT_ID, null, null, null, null, true);
        }
        return new AwsSelection(
                DEFAULT_ACCOUNT_ID,
                global.accessKey(),
                global.secretKey(),
                blankToNull(global.sessionToken()),
                null,
                false);
    }

    private static ProviderAccount pickHuaweiAccount(ProviderConfig mapping) {
        if (mapping.provider() != ProviderChannel.HUAWEI) {
            return null;
        }
        List<ProviderAccount> eligible = mapping.accounts().stream()
                .filter(ProviderAccount::hasHuaweiApiKey)
                .toList();
        if (eligible.isEmpty()) {
            return null;
        }
        return eligible.get(ThreadLocalRandom.current().nextInt(eligible.size()));
    }

    private static ProviderAccount pickAwsAccount(ProviderConfig mapping) {
        if (mapping.provider() != ProviderChannel.AWS) {
            return null;
        }
        List<ProviderAccount> eligible = mapping.accounts().stream()
                .filter(ProviderAccount::hasAwsCredentials)
                .toList();
        if (eligible.isEmpty()) {
            return null;
        }
        return eligible.get(ThreadLocalRandom.current().nextInt(eligible.size()));
    }

    private static String blankToNull(String value) {
        return value == null || value.isBlank() ? null : value;
    }
}
