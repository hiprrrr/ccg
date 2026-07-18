package com.padb.ccg.proxy;

import com.padb.ccg.core.exception.ProviderException;

import java.util.List;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

/**
 * 从 other-provider 的 {@code api-keys} 中均匀随机选取一个上游 Key。
 * <p>使用 {@link ThreadLocalRandom#nextInt(int)}，在有效 key 池内每个条目被选中的概率相等。</p>
 */
public final class UpstreamAccountSelector {

    private UpstreamAccountSelector() {
    }

    /**
     * 账号选择结果。
     *
     * @param accountId 账号标识（日志用，不含密钥）
     * @param apiKey    实际上游 API Key
     */
    public record AccountSelection(String accountId, String apiKey) {
    }

    /**
     * 为 HTTP 透传供应商随机选取一个 Key；空白条目不会进入候选池。
     */
    public static AccountSelection select(OtherProviderItem provider) {
        return select(provider, Set.of());
    }

    /**
     * 为 HTTP 透传供应商随机选取一个 Key，排除已失败过的 Key（用于故障转移重试）。
     *
     * @param provider     供应商配置
     * @param excludedKeys 本轮请求中已尝试并失败的 Key
     */
    public static AccountSelection select(OtherProviderItem provider, Set<String> excludedKeys) {
        if (provider == null) {
            throw new ProviderException("Other provider config is required for account selection");
        }
        List<String> eligible = provider.eligibleApiKeys().stream()
                .filter(k -> excludedKeys == null || !excludedKeys.contains(k))
                .toList();
        if (eligible.isEmpty()) {
            throw new ProviderException("api-keys is not configured for provider '" + provider.name() + "'");
        }
        int index = ThreadLocalRandom.current().nextInt(eligible.size());
        String picked = eligible.get(index);
        String accountId = provider.name() + "-" + Integer.toHexString(picked.hashCode());
        return new AccountSelection(accountId, picked);
    }
}
