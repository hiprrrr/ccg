package com.padb.ccg.proxy;

import com.padb.ccg.core.model.ProviderConfig;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.ArrayList;
import java.util.List;

/**
 * 模型映射配置，绑定 {@code model-mappings} 列表。
 * 每条在 {@code model-name} 下声明 {@code provider}（{@code aws} 或 other-providers 中的 name）及同级上游参数。
 */
@ConfigurationProperties(prefix = "model-mappings")
public class ModelMappingsProperties extends ArrayList<ModelMappingsProperties.ModelMappingItem> {

    /**
     * 将 YAML 映射展开为 {@link ProviderConfig} 列表。
     */
    public List<ProviderConfig> toProviderConfigs() {
        List<ProviderConfig> configs = new ArrayList<>(size());
        for (ModelMappingItem item : this) {
            configs.add(item.toProviderConfig());
        }
        return configs;
    }

    /**
     * 单条模型映射：{@code model-name} 下包含 {@code provider} 与 {@code upstream-model-id} 等同级字段。
     */
    public record ModelMappingItem(String modelName, String provider, String upstreamModelId,
                                    String region, List<String> capabilities) {

        public ModelMappingItem {
            if (capabilities == null) {
                capabilities = List.of();
            } else {
                capabilities = List.copyOf(capabilities);
            }
        }

        ProviderConfig toProviderConfig() {
            return new ProviderConfig(provider, modelName, upstreamModelId, region, capabilities);
        }
    }
}
