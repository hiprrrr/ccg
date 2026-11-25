package com.padb.ccg.proxy;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.springframework.http.codec.ServerSentEvent;

import static org.assertj.core.api.Assertions.assertThat;

class AnthropicMessageSseEventsTest {

    private final ObjectMapper mapper = new ObjectMapper();

    @Test
    void expandsTextMessageIntoAnthropicSseEvents() throws Exception {
        String message = """
                {"id":"msg_1","type":"message","role":"assistant","model":"claude-x",
                 "content":[{"type":"text","text":"hello"}],
                 "stop_reason":"end_turn","usage":{"input_tokens":11,"output_tokens":7}}""";

        var events = AnthropicMessageSseEvents.fromMessage(mapper, message, "claude-x");

        assertThat(events).extracting(ServerSentEvent::event)
                .containsExactly("message_start", "content_block_start", "content_block_delta", "content_block_stop",
                        "message_delta", "message_stop");

        var start = mapper.readTree(events.get(0).data());
        assertThat(start.path("message").path("id").asText()).isEqualTo("msg_1");
        assertThat(start.path("message").path("model").asText()).isEqualTo("claude-x");
        assertThat(start.path("message").path("usage").path("input_tokens").asInt()).isEqualTo(11);

        var delta = mapper.readTree(events.get(2).data());
        assertThat(delta.path("index").asInt()).isZero();
        assertThat(delta.path("delta").path("type").asText()).isEqualTo("text_delta");
        assertThat(delta.path("delta").path("text").asText()).isEqualTo("hello");

        var messageDelta = mapper.readTree(events.get(4).data());
        assertThat(messageDelta.path("delta").path("stop_reason").asText()).isEqualTo("end_turn");
        assertThat(messageDelta.path("usage").path("output_tokens").asInt()).isEqualTo(7);
    }

    @Test
    void expandsToolUseIntoInputJsonDelta() throws Exception {
        String message = """
                {"id":"msg_2","type":"message","role":"assistant",
                 "content":[{"type":"tool_use","id":"toolu_1","name":"Read","input":{"path":"a.png"}}],
                 "stop_reason":"tool_use","usage":{"input_tokens":3,"output_tokens":5}}""";

        var events = AnthropicMessageSseEvents.fromMessage(mapper, message, "m");

        var blockStart = mapper.readTree(events.get(1).data());
        assertThat(blockStart.path("content_block").path("type").asText()).isEqualTo("tool_use");
        assertThat(blockStart.path("content_block").path("id").asText()).isEqualTo("toolu_1");
        assertThat(blockStart.path("content_block").path("name").asText()).isEqualTo("Read");

        var delta = mapper.readTree(events.get(2).data());
        assertThat(delta.path("delta").path("type").asText()).isEqualTo("input_json_delta");
        assertThat(delta.path("delta").path("partial_json").asText()).isEqualTo("{\"path\":\"a.png\"}");
    }

    @Test
    void expandedEventsRebuildOriginalMessageThroughAggregator() throws Exception {
        String message = """
                {"id":"msg_3","type":"message","role":"assistant",
                 "content":[{"type":"text","text":"hello"},
                            {"type":"tool_use","id":"toolu_1","name":"Read","input":{"path":"a.png"}}],
                 "stop_reason":"tool_use","usage":{"input_tokens":3,"output_tokens":5}}""";

        var events = AnthropicMessageSseEvents.fromMessage(mapper, message, "m");
        var aggregator = new AnthropicSseAggregator(mapper);
        events.forEach(event -> aggregator.consume(event.data()));
        var rebuilt = aggregator.buildMessage();

        assertThat(rebuilt.path("id").asText()).isEqualTo("msg_3");
        assertThat(rebuilt.path("stop_reason").asText()).isEqualTo("tool_use");
        assertThat(rebuilt.path("usage").path("output_tokens").asInt()).isEqualTo(5);
        assertThat(rebuilt.path("content").get(0).path("text").asText()).isEqualTo("hello");
        assertThat(rebuilt.path("content").get(1).path("type").asText()).isEqualTo("tool_use");
        assertThat(rebuilt.path("content").get(1).path("input").path("path").asText()).isEqualTo("a.png");
    }

    @Test
    void skipsUnknownBlockTypes() throws Exception {
        String message = """
                {"id":"msg_4","type":"message","role":"assistant",
                 "content":[{"type":"server_tool_use","id":"srv_1","name":"web_search"}],
                 "stop_reason":"end_turn","usage":{"input_tokens":1,"output_tokens":1}}""";

        var events = AnthropicMessageSseEvents.fromMessage(mapper, message, "m");

        assertThat(events).extracting(ServerSentEvent::event)
                .containsExactly("message_start", "message_delta", "message_stop");
    }
}
