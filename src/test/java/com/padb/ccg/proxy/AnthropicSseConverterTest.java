package com.padb.ccg.proxy;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.springframework.http.codec.ServerSentEvent;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@link AnthropicSseConverter} 的单元测试：覆盖流式结束无 finish_reason 时的补尾逻辑。
 */
class AnthropicSseConverterTest {

    private final ObjectMapper mapper = new ObjectMapper();

    @Test
    void endStreamIfOpen_appendsStopEventsWhenStillStreaming() {
        AnthropicSseConverter c = new AnthropicSseConverter(mapper, "claude-sonnet-4-6");
        List<ServerSentEvent<String>> start = c.convert("""
                {"choices":[{"delta":{"content":"hi"},"finish_reason":null}]}""");
        assertThat(start).hasSizeGreaterThanOrEqualTo(2);

        List<ServerSentEvent<String>> tail = c.endStreamIfOpen();
        assertThat(tail).hasSize(3);
        assertThat(tail.get(0).event()).isEqualTo("content_block_stop");
        assertThat(tail.get(1).event()).isEqualTo("message_delta");
        assertThat(tail.get(2).event()).isEqualTo("message_stop");

        assertThat(c.endStreamIfOpen()).isEmpty();
    }

    @Test
    void endStreamIfOpen_noopWhenAlreadyDoneViaFinishReason() {
        AnthropicSseConverter c = new AnthropicSseConverter(mapper, "m");
        c.convert("""
                {"choices":[{"delta":{"content":"x"},"finish_reason":"stop"}]}""");
        assertThat(c.endStreamIfOpen()).isEmpty();
    }

    @Test
    void endStreamIfOpen_synthesizesFullAnthropicTailWhenStillInInitialState() {
        AnthropicSseConverter c = new AnthropicSseConverter(mapper, "m");
        List<ServerSentEvent<String>> tail = c.endStreamIfOpen();
        assertThat(tail).extracting(ServerSentEvent::event)
                .containsExactly("message_start", "content_block_start", "content_block_stop",
                        "message_delta", "message_stop");
        assertThat(c.endStreamIfOpen()).isEmpty();
    }

    @Test
    void convert_finalToolArgumentsInSameChunkAsFinishReason_areNotDropped() {
        AnthropicSseConverter c = new AnthropicSseConverter(mapper, "m");
        c.convert("""
                {"choices":[{"delta":{"tool_calls":[{"index":0,"id":"call_x","function":{"name":"Read","arguments":"{\\"file_path\\":\\"/tmp/a"}}]},"finish_reason":null}]}""");

        List<ServerSentEvent<String>> last = c.convert("""
                {"choices":[{"delta":{"tool_calls":[{"index":0,"function":{"arguments":".yml\\"}"}}]},"finish_reason":"tool_calls"}]}""");

        String blob = last.stream().map(ServerSentEvent::data).reduce("", String::concat);
        assertThat(blob).contains("input_json_delta");
        assertThat(blob).contains(".yml");
        assertThat(blob).contains("content_block_stop");
    }

    @Test
    void convert_streamsOpenAiToolCallsAsAnthropicToolUse() {
        AnthropicSseConverter c = new AnthropicSseConverter(mapper, "m");

        List<ServerSentEvent<String>> first = c.convert("""
                {"choices":[{"delta":{"tool_calls":[{"index":0,"id":"call_1","function":{"name":"Read","arguments":"{\\"path\\""}}]},"finish_reason":null}]}""");
        assertThat(first).extracting(ServerSentEvent::event)
                .contains("message_start", "content_block_start", "content_block_delta");
        assertThat(first.get(1).data()).contains("\"type\":\"tool_use\"");
        assertThat(first.get(1).data()).contains("\"id\":\"call_1\"");
        assertThat(first.get(1).data()).contains("\"name\":\"Read\"");
        assertThat(first.get(2).data()).contains("\"input_json_delta\"");

        List<ServerSentEvent<String>> second = c.convert("""
                {"choices":[{"delta":{"tool_calls":[{"index":0,"function":{"arguments":" : \\"pom.xml\\"}"}}]},"finish_reason":null}]}""");
        assertThat(second).extracting(ServerSentEvent::event).containsExactly("content_block_delta");

        List<ServerSentEvent<String>> done = c.convert("""
                {"choices":[{"delta":{},"finish_reason":"tool_calls"}]}""");
        assertThat(done).extracting(ServerSentEvent::event)
                .containsExactly("content_block_stop", "message_delta", "message_stop");
        assertThat(done.get(1).data()).contains("\"stop_reason\":\"tool_use\"");
    }

    @Test
    void convert_streamsOpenAiToolCallsWhenArgumentsIsJsonObject() {
        AnthropicSseConverter c = new AnthropicSseConverter(mapper, "m");
        List<ServerSentEvent<String>> first = c.convert("""
                {"choices":[{"delta":{"tool_calls":[{"index":0,"id":"call_b","function":{"name":"bash","arguments":{"command":"cat README"}}}]},"finish_reason":null}]}""");
        assertThat(first).extracting(ServerSentEvent::event)
                .contains("message_start", "content_block_start", "content_block_delta");
        assertThat(first.stream().filter(e -> "content_block_delta".equals(e.event()))
                .map(ServerSentEvent::data)
                .anyMatch(d -> d.contains("input_json_delta") && d.contains("command") && d.contains("cat README")))
                .isTrue();
    }

    @Test
    void convertCompleteMessage_handlesToolCallsWithObjectArguments() throws Exception {
        AnthropicSseConverter c = new AnthropicSseConverter(mapper, "m");
        List<ServerSentEvent<String>> events = c.convertCompleteMessage("""
                {"choices":[{"message":{"role":"assistant","content":null,
                "tool_calls":[{"id":"c1","type":"function","function":{"name":"Read","arguments":{"path":"/pom.xml"}}}]},
                "finish_reason":"tool_calls"}]}""");
        String blob = String.join("\n", events.stream().map(ServerSentEvent::data).toList());
        assertThat(blob).contains("pom.xml");
        assertThat(blob).contains("input_json_delta");
    }

    @Test
    void convertCompleteMessage_wrapsNonStreamingTextAsAnthropicSse() {
        AnthropicSseConverter c = new AnthropicSseConverter(mapper, "deepseek");
        List<ServerSentEvent<String>> events = c.convertCompleteMessage("""
                {"choices":[{"message":{"role":"assistant","content":"ok"},"finish_reason":"stop"}],
                 "usage":{"input_tokens":3,"output_tokens":2}}""");

        assertThat(events).extracting(ServerSentEvent::event)
                .containsExactly("message_start", "content_block_start", "content_block_delta",
                        "content_block_stop", "message_delta", "message_stop");
        assertThat(events.get(2).data()).contains("\"text\":\"ok\"");
        assertThat(events.get(4).data()).contains("\"stop_reason\":\"end_turn\"");
    }

    @Test
    void convertCompleteMessage_emitsEmptyTextBlockWhenModelReturnsNoContent() {
        AnthropicSseConverter c = new AnthropicSseConverter(mapper, "deepseek");
        List<ServerSentEvent<String>> events = c.convertCompleteMessage("""
                {"choices":[{"message":{"role":"assistant","content":""},"finish_reason":"stop"}]}""");

        assertThat(events).extracting(ServerSentEvent::event)
                .containsExactly("message_start", "content_block_start",
                        "content_block_stop", "message_delta", "message_stop");
        assertThat(events.get(1).data()).contains("\"type\":\"text\"");
    }
}
