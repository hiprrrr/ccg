package com.padb.ccg.routing;

import com.padb.ccg.core.model.ProviderConfig;
import com.padb.ccg.core.spi.ProviderRegistry;
import com.padb.ccg.proxy.ModelMappingsProperties;
import com.padb.ccg.proxy.OtherProvidersRegistry;
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
    private final OtherProvidersRegistry otherProvidersRegistry;

    /** 模型名称 → 供应商配置的映射表，原子引用保证线程安全的读写 */
    private final AtomicReference<Map<String, ProviderConfig>> modelMap =
            new AtomicReference<>(Map.of());

    public ProviderRegistryImpl(ModelMappingsProperties modelMappingsProperties,
                                OtherProvidersRegistry otherProvidersRegistry) {
        this.modelMappingsProperties = modelMappingsProperties;
        this.otherProvidersRegistry = otherProvidersRegistry;
    }

    /** 启动时从配置加载模型映射，并校验非 aws 的 provider 在 other-providers 中存在 */
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
            validateProviderReference(c);
            ProviderConfig existing = newMap.put(c.modelName(), c);
            if (existing != null) {
                log.warn("Duplicate model '{}' mapped to both provider={}/{} and provider={}/{}; using last",
                        c.modelName(), existing.provider(), existing.upstreamModelId(),
                        c.provider(), c.upstreamModelId());
            }
        }
        modelMap.set(Collections.unmodifiableMap(newMap));
        long awsCount = newMap.values().stream().filter(ProviderConfig::isAws).count();
        long otherCount = newMap.size() - awsCount;
        log.info("Model registry rebuilt: {} mapping(s) (aws={}, other={})", newMap.size(), awsCount, otherCount);
    }

    /** 获取当前注册的模型数量 */
    public int modelCount() {
        return modelMap.get().size();
    }

    /**
     * 非 aws 的 provider 必须能在 other-providers 中解析到。
     */
    private void validateProviderReference(ProviderConfig config) {
        if (config.isAws()) {
            return;
        }
        if (otherProvidersRegistry.find(config.provider()).isEmpty()) {
            throw new IllegalStateException(
                    "model-mappings model '" + config.modelName() + "' references unknown provider '"
                            + config.provider() + "' — declare it under other-providers");
        }
    }
}
