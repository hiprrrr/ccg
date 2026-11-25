package com.padb.ccg.proxy;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.springframework.http.codec.ServerSentEvent;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class NovaSseConverterTest {

    private final ObjectMapper mapper = new ObjectMapper();

    /** 模拟 Nova 文本流：contentBlockStart → 两个 delta → stop → messageStop → metadata */
    private List<String> textStreamChunks() {
        return List.of(
                "{\"contentBlockStart\":{\"start\":{\"text\":\"\"},\"contentBlockIndex\":0}}",
                "{\"contentBlockDelta\":{\"delta\":{\"text\":\"Hel\"},\"contentBlockIndex\":0}}",
                "{\"contentBlockDelta\":{\"delta\":{\"text\":\"lo\"},\"contentBlockIndex\":0}}",
                "{\"contentBlockStop\":{\"contentBlockIndex\":0}}",
                "{\"messageStop\":{\"stopReason\":\"end_turn\"}}",
                "{\"metadata\":{\"usage\":{\"inputTokens\":11,\"outputTokens\":7,\"totalTokens\":18}}}");
    }

    @Test
    void convertsTextStreamToAnthropicSse() throws Exception {
        var converter = new NovaSseConverter(mapper, "nova-pro");
        List<ServerSentEvent<String>> events = new ArrayList<>();
        for (String chunk : textStreamChunks()) {
            events.addAll(converter.convert(chunk));
        }
        events.addAll(converter.endStreamIfOpen());

        assertThat(events).extracting(ServerSentEvent::event).containsExactly(
                "message_start", "content_block_start", "content_block_delta", "content_block_delta",
                "content_block_stop", "message_delta", "message_stop");

        var aggregator = new AnthropicSseAggregator(mapper);
        events.forEach(e -> aggregator.consume(e.data()));
        var message = aggregator.buildMessage();

        assertThat(message.path("content").get(0).path("text").asText()).isEqualTo("Hello");
        assertThat(message.path("stop_reason").asText()).isEqualTo("end_turn");
        assertThat(message.path("usage").path("output_tokens").asInt()).isEqualTo(7);
        // Nova 的真实 input_tokens 在流末 message_delta.usage，聚合器应覆盖 message_start 里的 0
        assertThat(message.path("usage").path("input_tokens").asInt()).isEqualTo(11);

        assertThat(converter.getInputTokens()).isEqualTo(11);
        assertThat(converter.getOutputTokens()).isEqualTo(7);
    }

    @Test
    void convertsToolUseStreamToAnthropicSse() throws Exception {
        var converter = new NovaSseConverter(mapper, "nova-pro");
        List<ServerSentEvent<String>> events = new ArrayList<>();
        for (String chunk : List.of(
                "{\"contentBlockStart\":{\"start\":{\"text\":\"\"},\"contentBlockIndex\":0}}",
                "{\"contentBlockDelta\":{\"delta\":{\"text\":\"reading\"},\"contentBlockIndex\":0}}",
                "{\"contentBlockStop\":{\"contentBlockIndex\":0}}",
                "{\"contentBlockStart\":{\"start\":{\"toolUse\":{\"toolUseId\":\"toolu_1\",\"name\":\"Read\"}},\"contentBlockIndex\":1}}",
                "{\"contentBlockDelta\":{\"delta\":{\"toolUse\":{\"input\":\"{\\\"path\\\":\\\"a.png\\\"}\"}},\"contentBlockIndex\":1}}",
                "{\"contentBlockStop\":{\"contentBlockIndex\":1}}",
                "{\"messageStop\":{\"stopReason\":\"tool_use\"}}")) {
            events.addAll(converter.convert(chunk));
        }
        events.addAll(converter.endStreamIfOpen());

        var aggregator = new AnthropicSseAggregator(mapper);
        events.forEach(e -> aggregator.consume(e.data()));
        var message = aggregator.buildMessage();

        assertThat(message.path("content").get(0).path("text").asText()).isEqualTo("reading");
        var toolUse = message.path("content").get(1);
        assertThat(toolUse.path("type").asText()).isEqualTo("tool_use");
        assertThat(toolUse.path("id").asText()).isEqualTo("toolu_1");
        assertThat(toolUse.path("name").asText()).isEqualTo("Read");
        assertThat(toolUse.path("input").path("path").asText()).isEqualTo("a.png");
        assertThat(message.path("stop_reason").asText()).isEqualTo("tool_use");
    }

    @Test
    void synthesizesTailWhenStreamEndsWithoutMessageStop() throws Exception {
        var converter = new NovaSseConverter(mapper, "nova-pro");
        List<ServerSentEvent<String>> events = new ArrayList<>();
        for (String chunk : List.of(
                "{\"contentBlockStart\":{\"start\":{\"text\":\"\"},\"contentBlockIndex\":0}}",
                "{\"contentBlockDelta\":{\"delta\":{\"text\":\"hi\"},\"contentBlockIndex\":0}}")) {
            events.addAll(converter.convert(chunk));
        }
        events.addAll(converter.endStreamIfOpen());

        assertThat(events).extracting(ServerSentEvent::event).containsExactly(
                "message_start", "content_block_start", "content_block_delta",
                "content_block_stop", "message_delta", "message_stop");
    }

    @Test
    void mapsMaxTokensStopReason() throws Exception {
        var converter = new NovaSseConverter(mapper, "nova-pro");
        List<ServerSentEvent<String>> events = new ArrayList<>();
        for (String chunk : List.of(
                "{\"contentBlockStart\":{\"start\":{\"text\":\"\"},\"contentBlockIndex\":0}}",
                "{\"contentBlockDelta\":{\"delta\":{\"text\":\"abc\"},\"contentBlockIndex\":0}}",
                "{\"contentBlockStop\":{\"contentBlockIndex\":0}}",
                "{\"messageStop\":{\"stopReason\":\"max_tokens\"}}")) {
            events.addAll(converter.convert(chunk));
        }
        events.addAll(converter.endStreamIfOpen());

        var aggregator = new AnthropicSseAggregator(mapper);
        events.forEach(e -> aggregator.consume(e.data()));
        assertThat(aggregator.buildMessage().path("stop_reason").asText()).isEqualTo("max_tokens");
    }

    @Test
    void convertsNonStreamingResponseToAnthropicMessage() throws Exception {
        String novaResponse = """
                {"output":{"message":{"role":"assistant","content":[
                   {"text":"hello"},
                   {"toolUse":{"toolUseId":"toolu_1","name":"Read","input":{"path":"a.png"}}}
                 ]}},
                 "stopReason":"tool_use",
                 "usage":{"inputTokens":3,"outputTokens":5}}
                """;

        String anthropicJson = NovaSseConverter.toAnthropicMessage(mapper, novaResponse, "nova-pro");
        JsonNode message = mapper.readTree(anthropicJson);

        assertThat(message.path("type").asText()).isEqualTo("message");
        assertThat(message.path("role").asText()).isEqualTo("assistant");
        assertThat(message.path("model").asText()).isEqualTo("nova-pro");
        assertThat(message.path("content").get(0).path("text").asText()).isEqualTo("hello");
        assertThat(message.path("content").get(1).path("type").asText()).isEqualTo("tool_use");
        assertThat(message.path("content").get(1).path("input").path("path").asText()).isEqualTo("a.png");
        assertThat(message.path("stop_reason").asText()).isEqualTo("tool_use");
        assertThat(message.path("usage").path("input_tokens").asInt()).isEqualTo(3);
        assertThat(message.path("usage").path("output_tokens").asInt()).isEqualTo(5);

        // 展开成 SSE 后能被聚合器还原
        var events = AnthropicMessageSseEvents.fromMessage(mapper, anthropicJson, "nova-pro");
        var aggregator = new AnthropicSseAggregator(mapper);
        events.forEach(e -> aggregator.consume(e.data()));
        var rebuilt = aggregator.buildMessage();
        assertThat(rebuilt.path("content").get(1).path("input").path("path").asText()).isEqualTo("a.png");
        assertThat(rebuilt.path("stop_reason").asText()).isEqualTo("tool_use");
    }

    @Test
    void ignoresUnparseableChunk() {
        var converter = new NovaSseConverter(mapper, "nova-pro");
        assertThat(converter.convert("not json")).isEmpty();
        assertThat(converter.convert((String) null)).isEmpty();
    }
}
