package com.padb.ccg.proxy;

import com.padb.ccg.core.exception.ProviderException;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

/**
 * 按 {@code name} 查找 {@code other-providers} 中的 HTTP 透传供应商配置。
 */
@Component
public class OtherProvidersRegistry {

    private static final Logger log = LoggerFactory.getLogger(OtherProvidersRegistry.class);

    private final OtherProvidersProperties properties;
    private final AtomicReference<Map<String, OtherProviderItem>> byName =
            new AtomicReference<>(Map.of());

    public OtherProvidersRegistry(OtherProvidersProperties properties) {
        this.properties = properties;
    }

    @PostConstruct
    void init() {
        rebuild(properties);
    }

    /**
     * 重建名称 → 配置映射；名称为空或重复时打警告并跳过/覆盖。
     */
    public void rebuild(OtherProvidersProperties items) {
        Map<String, OtherProviderItem> map = new HashMap<>();
        if (items != null) {
            for (OtherProviderItem item : items) {
                if (item == null || item.name() == null || item.name().isBlank()) {
                    log.warn("Skipping other-providers entry with blank name");
                    continue;
                }
                String key = normalizeKey(item.name());
                OtherProviderItem existing = map.put(key, item);
                if (existing != null) {
                    log.warn("Duplicate other-providers name '{}'; using last entry", item.name());
                }
                warnEmptyApiKeys(item);
            }
        }
        byName.set(Collections.unmodifiableMap(map));
        log.info("Other providers registry rebuilt: {} provider(s)", map.size());
    }

    /** 按名称查找（大小写不敏感） */
    public Optional<OtherProviderItem> find(String name) {
        if (name == null || name.isBlank()) {
            return Optional.empty();
        }
        return Optional.ofNullable(byName.get().get(normalizeKey(name)));
    }

    /** 查找且必须存在，否则抛出 ProviderException */
    public OtherProviderItem require(String name) {
        return find(name).orElseThrow(() -> new ProviderException(
                "Unknown other provider '" + name + "' — not found in other-providers list"));
    }

    /** 当前已注册的供应商数量 */
    public int size() {
        return byName.get().size();
    }

    /**
     * 若配置了 api-keys 但部分为空，启动/热更新时告警，避免误以为在均匀分流。
     */
    private static void warnEmptyApiKeys(OtherProviderItem provider) {
        List<String> configured = provider.apiKeys();
        if (configured.isEmpty()) {
            return;
        }
        List<String> eligible = provider.eligibleApiKeys();
        if (eligible.size() < configured.size()) {
            log.warn("other-providers '{}' api-keys: {} configured, {} non-blank — "
                            + "empty keys are excluded from random pool",
                    provider.name(), configured.size(), eligible.size());
        }
    }

    private static String normalizeKey(String name) {
        return name.trim().toLowerCase(Locale.ROOT);
    }
}
