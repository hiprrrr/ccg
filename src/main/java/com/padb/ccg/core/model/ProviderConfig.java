package com.padb.ccg.core.model;

import java.util.List;

/**
 * 供应商配置，定义用户请求的模型名称到上游模型的映射关系。
 * 以 {@code modelName} 作为网关侧唯一识别键；{@code upstreamModelId} 为实际调用上游时使用的模型标识。
 *
 * @param provider         上游渠道名：{@code aws} 或 {@code other-providers} 列表中的 {@code name}
 * @param modelName        用户面向的模型名称
 * @param upstreamModelId  上游模型 ID（Bedrock modelId 或 HTTP 透传供应商的 model 参数）
 * @param region           AWS 区域（仅 aws 渠道使用，其他可为 null）
 * @param capabilities     模型能力列表（如 vision、tools、stream 等）
 */
public record ProviderConfig(String provider, String modelName, String upstreamModelId,
                              String region, List<String> capabilities) {
    public ProviderConfig {
        if (provider == null || provider.isBlank()) {
            provider = ProviderNames.AWS;
        } else {
            provider = provider.trim();
        }
        if (capabilities == null) {
            capabilities = List.of();
        } else {
            capabilities = List.copyOf(capabilities);
        }
    }

    /** 是否走 AWS Bedrock */
    public boolean isAws() {
        return ProviderNames.isAws(provider);
    }

    /** 是否声明了视觉能力（content 中可含 image / image_url 块） */
    public boolean supportsVision() {
        return capabilities != null && capabilities.contains("vision");
    }
}
