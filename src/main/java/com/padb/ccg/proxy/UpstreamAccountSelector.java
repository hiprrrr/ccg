package com.padb.ccg.proxy;

import com.padb.ccg.core.model.ProviderAccount;
import com.padb.ccg.core.model.ProviderChannel;
import com.padb.ccg.core.model.ProviderConfig;
import com.padb.ccg.core.exception.ProviderException;

import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

/**
 * 按华为 MaaS 模型映射从多 Key 池中均匀随机选取一个上游账号；未配置 accounts 时回退到全局 api-key。
 * <p>使用 {@link ThreadLocalRandom#nextInt(int)}，在有效账号池内每个条目被选中的概率相等。</p>
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
     * 为华为 MaaS 随机选取一个账号；模型未配置 accounts 时使用 {@code huawei-maas.api-key}。
     * api-key 为空的条目不会进入候选池。
     */
    public static HuaweiSelection selectHuawei(ProviderConfig mapping, HuaweiMaasProperties global) {
        if (mapping.provider() != ProviderChannel.HUAWEI) {
            throw new ProviderException("Upstream account selection is only supported for Huawei MaaS");
        }
        List<ProviderAccount> eligible = eligibleHuaweiAccounts(mapping);
        if (!eligible.isEmpty()) {
            int index = ThreadLocalRandom.current().nextInt(eligible.size());
            ProviderAccount picked = eligible.get(index);
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

    /** 返回配置了非空 api-key 的华为账号列表（保持 YAML 中的顺序） */
    static List<ProviderAccount> eligibleHuaweiAccounts(ProviderConfig mapping) {
        return mapping.accounts().stream()
                .filter(ProviderAccount::hasApiKey)
                .toList();
    }
}
