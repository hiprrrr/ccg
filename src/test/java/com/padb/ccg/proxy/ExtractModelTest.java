package com.padb.ccg.proxy;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 验证共享组件的 extractModel 只取顶层 model 字段，
 * 不被 messages/metadata 中嵌套的 model 键干扰。
 */
class ExtractModelTest {

    private final ObjectMapper mapper = new ObjectMapper();
    private final ProxyRequestSupport support = new ProxyRequestSupport(null, null, null, mapper);

    /** metadata 中的嵌套 model 键出现在顶层 model 之前，字符串扫描会误取嵌套值 */
    private static final String BODY_WITH_NESTED_MODEL = """
            {"metadata":{"model":"nested-fake"},"model":"real-model","messages":[{"role":"user","content":"hi"}]}
            """;

    @Test
    void extractsTopLevelModel() {
        assertThat(support.extractModel(BODY_WITH_NESTED_MODEL)).isEqualTo("real-model");
    }

    @Test
    void returnsNullWhenModelMissingOrNotTextual() {
        assertThat(support.extractModel("{\"messages\":[]}")).isNull();
        assertThat(support.extractModel("{\"model\":123}")).isNull();
        assertThat(support.extractModel("not json")).isNull();
    }
}
