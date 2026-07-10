package com.padb.ccg.routing;

import com.padb.ccg.core.model.ProviderAccount;
import com.padb.ccg.core.model.ProviderChannel;
import com.padb.ccg.core.model.ProviderConfig;
import com.padb.ccg.core.spi.ProviderRegistry;
import com.padb.ccg.proxy.ModelMappingsProperties;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

/**
 * 供应商注册表实现，维护模型名称到上游配置的映射表。
 * 映射来源为 {@code model-mappings} 列表：每条在 {@code model-name} 下声明 {@code provider} 及同级上游参数。
 */
@Component
public class ProviderRegistryImpl implements ProviderRegistry {

    private static final Logger log = LoggerFactory.getLogger(ProviderRegistryImpl.class);

    private final ModelMappingsProperties modelMappingsProperties;

    /** 模型名称 → 供应商配置的映射表，原子引用保证线程安全的读写 */
    private final AtomicReference<Map<String, ProviderConfig>> modelMap =
            new AtomicReference<>(Map.of());

    public ProviderRegistryImpl(ModelMappingsProperties modelMappingsProperties) {
        this.modelMappingsProperties = modelMappingsProperties;
    }

    /** 启动时从配置加载 AWS / 华为云模型映射 */
    @PostConstruct
    void init() {
        List<ProviderConfig> configs = modelMappingsProperties.toProviderConfigs();
        if (configs.isEmpty()) {
            log.warn("No model-mappings configured — gateway will reject all requests");
        }
        rebuild(configs);
    }

    @Override
    public Optional<ProviderConfig> resolve(String modelName) {
        return Optional.ofNullable(modelMap.get().get(modelName));
    }

    /**
     * 重建模型映射表（用于配置热更新）
     * 若存在重复的模型名称，以后出现的配置为准，并记录警告日志
     *
     * @param configs 新的供应商配置列表
     */
    public void rebuild(List<ProviderConfig> configs) {
        Map<String, ProviderConfig> newMap = new HashMap<>();
        for (ProviderConfig c : configs) {
            ProviderConfig existing = newMap.put(c.modelName(), c);
            if (existing != null) {
                log.warn("Duplicate model '{}' mapped to both provider={}/{} and provider={}/{}; using last",
                        c.modelName(), existing.provider(), existing.upstreamModelId(),
                        c.provider(), c.upstreamModelId());
            }
            warnHuaweiAccountsWithEmptyKeys(c);
        }
        modelMap.set(Collections.unmodifiableMap(newMap));
        long awsCount = newMap.values().stream().filter(c -> c.provider() == com.padb.ccg.core.model.ProviderChannel.AWS).count();
        long huaweiCount = newMap.size() - awsCount;
        log.info("Model registry rebuilt: {} mapping(s) (aws={}, huawei={})", newMap.size(), awsCount, huaweiCount);
    }

    /** 获取当前注册的模型数量 */
    public int modelCount() {
        return modelMap.get().size();
    }

    /**
     * 华为模型若配置了 accounts 但部分 api-key 为空，启动/热更新时告警，避免误以为在均匀分流。
     */
    private static void warnHuaweiAccountsWithEmptyKeys(ProviderConfig config) {
        if (config.provider() != ProviderChannel.HUAWEI || !config.hasAccounts()) {
            return;
        }
        List<ProviderAccount> accounts = config.accounts();
        long validCount = accounts.stream().filter(ProviderAccount::hasApiKey).count();
        if (validCount < accounts.size()) {
            List<String> skippedIds = accounts.stream()
                    .filter(a -> !a.hasApiKey())
                    .map(a -> a.resolvedId("?"))
                    .toList();
            log.warn("Model '{}' huawei accounts: {} configured, {} with api-key; skipped ids={} — "
                            + "empty keys are excluded from random pool",
                    config.modelName(), accounts.size(), validCount, skippedIds);
        }
    }
}
