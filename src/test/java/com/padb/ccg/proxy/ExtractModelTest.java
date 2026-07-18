package com.padb.ccg.proxy;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 验证两个代理服务的 extractModel 只取顶层 model 字段，
 * 不被 messages/metadata 中嵌套的 model 键干扰。
 */
class ExtractModelTest {

    private final ObjectMapper mapper = new ObjectMapper();
    private final ProxyService proxyService = new ProxyService(null, null, null, null, null, mapper);
    private final OpenAiProxyService openAiProxyService =
            new OpenAiProxyService(null, null, null, null, null, null, mapper);

    /** metadata 中的嵌套 model 键出现在顶层 model 之前，字符串扫描会误取嵌套值 */
    private static final String BODY_WITH_NESTED_MODEL = """
            {"metadata":{"model":"nested-fake"},"model":"real-model","messages":[{"role":"user","content":"hi"}]}
            """;

    @Test
    void proxyServiceExtractsTopLevelModel() {
        assertThat(proxyService.extractModel(BODY_WITH_NESTED_MODEL)).isEqualTo("real-model");
    }

    @Test
    void openAiProxyServiceExtractsTopLevelModel() {
        assertThat(openAiProxyService.extractModel(BODY_WITH_NESTED_MODEL)).isEqualTo("real-model");
    }

    @Test
    void returnsNullWhenModelMissingOrNotTextual() {
        assertThat(proxyService.extractModel("{\"messages\":[]}")).isNull();
        assertThat(proxyService.extractModel("{\"model\":123}")).isNull();
        assertThat(proxyService.extractModel("not json")).isNull();
        assertThat(openAiProxyService.extractModel("{\"messages\":[]}")).isNull();
    }
}
