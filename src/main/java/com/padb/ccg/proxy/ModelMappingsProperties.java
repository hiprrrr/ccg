package com.padb.ccg.proxy;

import com.padb.ccg.core.model.ProviderConfig;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.ConstructorBinding;

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
     *
     * @param upstreamFormat 显式声明上游协议格式（openai / anthropic / nova），为空时按 upstream-model-id 推断；
     *                       仅 provider=aws 时生效
     */
    public record ModelMappingItem(String modelName, String provider, String upstreamModelId,
                                    String region, List<String> capabilities, String upstreamFormat) {

        /** Spring Boot 绑定用 canonical 构造器（存在多个构造器时必须显式标注）。 */
        @ConstructorBinding
        public ModelMappingItem {
            if (capabilities == null) {
                capabilities = List.of();
            } else {
                capabilities = List.copyOf(capabilities);
            }
            if (upstreamFormat != null && upstreamFormat.isBlank()) {
                upstreamFormat = null;
            }
        }

        /** 不显式指定上游格式的构造方式。 */
        public ModelMappingItem(String modelName, String provider, String upstreamModelId,
                                 String region, List<String> capabilities) {
            this(modelName, provider, upstreamModelId, region, capabilities, null);
        }

        ProviderConfig toProviderConfig() {
            return new ProviderConfig(provider, modelName, upstreamModelId, region, capabilities, upstreamFormat);
        }
    }
}
