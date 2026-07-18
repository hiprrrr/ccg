package com.padb.ccg.proxy;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 验证 AnthropicSseAggregator 能把 SSE 事件序列还原为完整 message JSON，
 * 覆盖文本块与 tool_use 块两种内容形态。
 */
class AnthropicSseAggregatorTest {

    private final ObjectMapper mapper = new ObjectMapper();

    @Test
    void shouldAggregateTextMessage() {
        var aggregator = new AnthropicSseAggregator(mapper);

        aggregator.consume("{\"type\":\"message_start\",\"message\":{\"id\":\"msg_1\",\"type\":\"message\","
                + "\"role\":\"assistant\",\"model\":\"claude-x\",\"content\":[],"
                + "\"usage\":{\"input_tokens\":12,\"output_tokens\":0}}}");
        aggregator.consume("{\"type\":\"content_block_start\",\"index\":0,"
                + "\"content_block\":{\"type\":\"text\",\"text\":\"\"}}");
        aggregator.consume("{\"type\":\"content_block_delta\",\"index\":0,"
                + "\"delta\":{\"type\":\"text_delta\",\"text\":\"你好\"}}");
        aggregator.consume("{\"type\":\"content_block_delta\",\"index\":0,"
                + "\"delta\":{\"type\":\"text_delta\",\"text\":\"，世界\"}}");
        aggregator.consume("{\"type\":\"content_block_stop\",\"index\":0}");
        aggregator.consume("{\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\","
                + "\"stop_sequence\":null},\"usage\":{\"output_tokens\":7}}");
        aggregator.consume("{\"type\":\"message_stop\"}");

        ObjectNode message = aggregator.buildMessage();
        assertThat(message.path("id").asText()).isEqualTo("msg_1");
        assertThat(message.path("type").asText()).isEqualTo("message");
        assertThat(message.path("model").asText()).isEqualTo("claude-x");
        assertThat(message.at("/content/0/type").asText()).isEqualTo("text");
        assertThat(message.at("/content/0/text").asText()).isEqualTo("你好，世界");
        assertThat(message.path("stop_reason").asText()).isEqualTo("end_turn");
        assertThat(message.at("/usage/input_tokens").asInt()).isEqualTo(12);
        assertThat(message.at("/usage/output_tokens").asInt()).isEqualTo(7);
    }

    @Test
    void shouldAggregateToolUseBlock() {
        var aggregator = new AnthropicSseAggregator(mapper);

        aggregator.consume("{\"type\":\"message_start\",\"message\":{\"id\":\"msg_2\",\"role\":\"assistant\","
                + "\"model\":\"m\",\"content\":[],\"usage\":{\"input_tokens\":3,\"output_tokens\":0}}}");
        aggregator.consume("{\"type\":\"content_block_start\",\"index\":0,"
                + "\"content_block\":{\"type\":\"tool_use\",\"id\":\"toolu_1\",\"name\":\"Read\"}}");
        aggregator.consume("{\"type\":\"content_block_delta\",\"index\":0,"
                + "\"delta\":{\"type\":\"input_json_delta\",\"partial_json\":\"{\\\"path\\\":\"}}");
        aggregator.consume("{\"type\":\"content_block_delta\",\"index\":0,"
                + "\"delta\":{\"type\":\"input_json_delta\",\"partial_json\":\"\\\"pom.xml\\\"}\"}}");
        aggregator.consume("{\"type\":\"content_block_stop\",\"index\":0}");
        aggregator.consume("{\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"tool_use\"},"
                + "\"usage\":{\"output_tokens\":9}}");

        ObjectNode message = aggregator.buildMessage();
        JsonNode block = message.at("/content/0");
        assertThat(block.path("type").asText()).isEqualTo("tool_use");
        assertThat(block.path("id").asText()).isEqualTo("toolu_1");
        assertThat(block.path("name").asText()).isEqualTo("Read");
        assertThat(block.at("/input/path").asText()).isEqualTo("pom.xml");
        assertThat(message.path("stop_reason").asText()).isEqualTo("tool_use");
    }

    @Test
    void shouldSkipUnparseableEventsAndStillBuild() {
        var aggregator = new AnthropicSseAggregator(mapper);

        aggregator.consume("not json");
        aggregator.consume("{\"type\":\"ping\"}");

        ObjectNode message = aggregator.buildMessage();
        assertThat(message.path("type").asText()).isEqualTo("message");
        assertThat(message.path("role").asText()).isEqualTo("assistant");
        assertThat(message.withArray("/content")).isEmpty();
    }
}
