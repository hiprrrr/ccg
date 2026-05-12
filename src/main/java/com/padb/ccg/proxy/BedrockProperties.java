package com.padb.ccg.proxy;

import com.padb.ccg.core.model.ProviderConfig;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.List;

/**
 * Bedrock 配置属性，绑定 {@code bedrock.*} 配置项
 *
 * @param region        默认 AWS 区域
 * @param accessKey     AWS 访问密钥
 * @param secretKey     AWS 秘密密钥
 * @param retryMax      最大重试次数，默认 3
 * @param timeoutSeconds 调用超时时间（秒），默认 120
 * @param modelMappings 模型映射列表，定义用户模型到 Bedrock 模型的对应关系
 */
@ConfigurationProperties(prefix = "bedrock")
public record BedrockProperties(String region, String accessKey, String secretKey,
                                 int retryMax, int timeoutSeconds,
                                 List<ProviderConfig> modelMappings) {
    public BedrockProperties {
        if (region == null || region.isBlank()) region = "us-east-1";
        if (retryMax <= 0) retryMax = 3;
        if (timeoutSeconds <= 0) timeoutSeconds = 120;
        if (modelMappings == null) modelMappings = List.of();
    }
}
