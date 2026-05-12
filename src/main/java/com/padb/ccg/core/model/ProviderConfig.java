package com.padb.ccg.core.model;

import java.util.List;

/**
 * 供应商配置，定义用户请求的模型名称到 Bedrock 模型的映射关系。
 * 每条配置包含 Bedrock 模型 ID、区域及模型能力列表。
 *
 * @param id             配置唯一标识
 * @param modelName      用户面向的模型名称
 * @param bedrockModelId Bedrock 模型 ID
 * @param region         AWS 区域
 * @param capabilities   模型能力列表（如 vision、tools 等）
 */
public record ProviderConfig(String id, String modelName, String bedrockModelId,
                              String region, List<String> capabilities) {
    public ProviderConfig {
        // 防御性拷贝，确保能力列表不可变
        capabilities = List.copyOf(capabilities);
    }
}
