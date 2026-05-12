package com.padb.ccg.routing;

import com.padb.ccg.core.model.ProviderConfig;
import com.padb.ccg.core.spi.ProviderRegistry;
import com.padb.ccg.proxy.BedrockProperties;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

/**
 * 供应商注册表实现，维护模型名称到 Bedrock 供应商配置的映射表。
 * 使用 {@link AtomicReference} 包装不可变 Map，支持配置热更新时的线程安全替换。
 */
@Component
public class ProviderRegistryImpl implements ProviderRegistry {

    private static final Logger log = LoggerFactory.getLogger(ProviderRegistryImpl.class);

    private final BedrockProperties bedrockProperties;

    /** 模型名称 → 供应商配置的映射表，原子引用保证线程安全的读写 */
    private final AtomicReference<Map<String, ProviderConfig>> modelMap =
            new AtomicReference<>(Map.of());

    public ProviderRegistryImpl(BedrockProperties bedrockProperties) {
        this.bedrockProperties = bedrockProperties;
    }

    /** 启动时从配置加载模型映射 */
    @PostConstruct
    void init() {
        List<ProviderConfig> configs = bedrockProperties.modelMappings();
        if (configs.isEmpty()) {
            log.warn("No bedrock.model-mappings configured — gateway will reject all requests");
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
                log.warn("Duplicate model '{}' mapped to both '{}' and '{}'; using last",
                        c.modelName(), existing.bedrockModelId(), c.bedrockModelId());
            }
        }
        // 原子替换为不可变 Map，保证读线程安全
        modelMap.set(Collections.unmodifiableMap(newMap));
        log.info("Bedrock model registry rebuilt: {} model mapping(s)", newMap.size());
    }

    /** 获取当前注册的模型数量 */
    public int modelCount() {
        return modelMap.get().size();
    }
}
