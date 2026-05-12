package com.padb.ccg.core.spi;

import com.padb.ccg.core.model.ProviderConfig;

import java.util.Optional;

/**
 * 模型供应商标识解析接口，负责将用户请求的模型名称映射到具体的 Bedrock 供应商配置
 */
public interface ProviderRegistry {

    /**
     * 根据模型名称查找对应的供应商配置
     *
     * @param modelName 用户请求的模型名称
     * @return 供应商配置 Optional，未找到时为空
     */
    Optional<ProviderConfig> resolve(String modelName);
}
