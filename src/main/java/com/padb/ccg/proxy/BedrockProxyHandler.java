package com.padb.ccg.proxy;

import com.padb.ccg.core.model.ProviderConfig;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import jakarta.annotation.PreDestroy;
import org.springframework.http.codec.ServerSentEvent;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.SignalType;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.core.retry.RetryPolicy;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.bedrockruntime.BedrockRuntimeAsyncClient;
import software.amazon.awssdk.services.bedrockruntime.model.InvokeModelRequest;
import software.amazon.awssdk.services.bedrockruntime.model.InvokeModelWithResponseStreamRequest;
import software.amazon.awssdk.services.bedrockruntime.model.InvokeModelWithResponseStreamResponseHandler;
import software.amazon.awssdk.services.bedrockruntime.model.ResponseStream;

import software.amazon.awssdk.http.async.SdkAsyncHttpClient;
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.time.Duration;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Bedrock 代理处理器，负责将请求转发到 AWS Bedrock 并通过 SSE 流式返回响应。
 *
 * 核心流程：
 * 1. 按区域缓存 {@link BedrockRuntimeAsyncClient} 实例（底层 Netty 读超时与 {@code timeout-seconds} 对齐，避免流式 chunk 间隔过长被默认 ~30s 读超时断开）
 * 2. 构建流式调用请求，通过 Flux.push 桥接 Bedrock 异步回调到 Reactor Flux
 * 3. 在 boundedElastic 调度器上执行阻塞性 AWS SDK 调用
 * 4. 实时提取响应中的 token 用量信息
 */
@Component
public class BedrockProxyHandler {

    private static final Logger log = LoggerFactory.getLogger(BedrockProxyHandler.class);
    /** 仅工具链：与 {@link BedrockProperties#toolCallTraceEnabled()} 配套 */
    private static final Logger toolTrace = LoggerFactory.getLogger("com.padb.ccg.proxy.ToolCallTrace");

    /**
     * 进程级默认凭证链（EC2/EKS/IRSA 等），可自动轮换短期凭证；勿在 destroy 中关闭，避免与 JVM 内其他用法冲突。
     */
    private static final AwsCredentialsProvider DEFAULT_CREDENTIALS_PROVIDER = DefaultCredentialsProvider.create();

    private final BedrockProperties props;
    private final ObjectMapper objectMapper;

    /** 按区域 + 账号缓存 BedrockAsyncClient，避免重复创建 */
    private final Map<String, BedrockRuntimeAsyncClient> clients = new ConcurrentHashMap<>();

    public BedrockProxyHandler(BedrockProperties props, ObjectMapper objectMapper) {
        this.props = props;
        this.objectMapper = objectMapper;
    }

    /** 容器销毁时关闭所有 Bedrock 客户端连接 */
    @PreDestroy
    void destroy() {
        clients.forEach((region, client) -> {
            try {
                client.close();
                log.info("Closed BedrockRuntimeAsyncClient for region={}", region);
            } catch (Exception e) {
                log.warn("Error closing Bedrock client for region={}", region, e);
            }
        });
        clients.clear();
    }

    /**
     * 创建带 Netty 读/写超时配置的异步 Bedrock 客户端。
     * <p>默认 Netty 客户端两次读事件间隔约 30s 即超时；大模型首包或 chunk 间隔可能超过该值，需与 {@link BedrockProperties#timeoutSeconds()} 一致。</p>
     */
    private BedrockRuntimeAsyncClient buildBedrockClient(String awsRegion, AwsCredentialsProvider credentialsProvider) {
        int sec = Math.max(1, props.timeoutSeconds());
        Duration d = Duration.ofSeconds(sec);
        SdkAsyncHttpClient http = NettyNioAsyncHttpClient.builder()
                .readTimeout(d)
                .writeTimeout(d)
                .connectionTimeout(Duration.ofSeconds(30))
                .build();
        return BedrockRuntimeAsyncClient.builder()
                .region(Region.of(awsRegion))
                .credentialsProvider(credentialsProvider)
                .httpClient(http)
                .overrideConfiguration(b -> b
                        .retryPolicy(RetryPolicy.builder().numRetries(props.retryMax()).build())
                        .apiCallTimeout(d))
                .build();
    }

    private BedrockRuntimeAsyncClient getBedrockClient(String awsRegion, UpstreamAccountSelector.AwsSelection account) {
        String cacheKey = awsRegion + ":" + account.accountId();
        return clients.computeIfAbsent(cacheKey, ignored -> buildBedrockClient(
                awsRegion, credentialsProviderFor(account)));
    }

    private AwsCredentialsProvider credentialsProviderFor(UpstreamAccountSelector.AwsSelection account) {
        if (account.useDefaultChain()) {
            return DEFAULT_CREDENTIALS_PROVIDER;
        }
        return () -> resolveCredentials(account);
    }

    /**
     * 根据账号选择结果解析 AWS 凭证：有 session token 时使用 STS 会话凭证，否则使用基本凭证。
     */
    private software.amazon.awssdk.auth.credentials.AwsCredentials resolveCredentials(
            UpstreamAccountSelector.AwsSelection account) {
        String token = account.sessionToken();
        if (token != null && !token.isBlank()) {
            return AwsSessionCredentials.create(account.accessKey(), account.secretKey(), token);
        }
        return AwsBasicCredentials.create(account.accessKey(), account.secretKey());
    }

    /**
     * 转发请求到 Bedrock 并返回 SSE 流
     *
     * @param mapping      模型映射配置
     * @param requestBody  请求体 JSON
     * @param inputTokens  输入 token 累加器（原子更新）
     * @param outputTokens 输出 token 累加器（原子更新）
     * @param username     用户名
     * @param model        用户请求的模型名称
     * @param traceId      与网关入口日志对齐的关联 ID（如 ProxyService 的 request id）
     * @return SSE 事件流
     */
    public Flux<ServerSentEvent<String>> forward(ProviderConfig mapping, String requestBody,
                                                  AtomicInteger inputTokens, AtomicInteger outputTokens,
                                                  String username, String model, String traceId) {

        UpstreamAccountSelector.AwsSelection account = UpstreamAccountSelector.selectAws(mapping, props);
        // 区域优先级：账号覆盖 > 模型映射 > 全局 bedrock.region
        String region = account.regionOverride() != null ? account.regionOverride()
                : (mapping.region() != null ? mapping.region() : props.region());

        // Anthropic 格式转换器（仅当 response-format=anthropic 时创建，惰性初始化）
        boolean useAnthropic = "anthropic".equalsIgnoreCase(props.responseFormat());
        AtomicReference<AnthropicSseConverter> converterRef = new AtomicReference<>();
        AtomicReference<CompletableFuture<Void>> invokeFutureRef = new AtomicReference<>();
        AtomicBoolean streamCompleted = new AtomicBoolean(false);
        /** 与 ProxyService 入口日志共用的关联 ID，便于把 Bedrock 原始 chunk 与一次 HTTP 请求对齐 */
        final String correlationId = (traceId == null || traceId.isBlank()) ? "-" : traceId;
        /** Bedrock SDK onChunk 收到的原始 UTF-8 块序号（从 0 递增） */
        final AtomicInteger rawChunkSeq = new AtomicInteger(0);

        // 使用 Flux.push 桥接 Bedrock 异步回调到 Reactor Flux
        // Flux.push 内部处理跨线程 emit 的序列化，比 Sinks.Many 更适合回调场景
        return Flux.<ServerSentEvent<String>>push(emitter -> {
                    // 按区域 + 账号获取或创建 BedrockAsyncClient
                    BedrockRuntimeAsyncClient client = getBedrockClient(region, account);

                    try {
                        // 去掉 model 字段后，规范化 metadata，避免上游 user_id 等违反 Bedrock 正则导致 400
                        String stripped = stripModelField(requestBody);
                        String bodyForBedrock;
                        try {
                            boolean toolsEnabled = supportsTools(mapping);
                            boolean streamingEnabled = supportsStreaming(mapping);
                            bodyForBedrock = BedrockInvokeBodyPreparer.normalizeMetadata(
                                    objectMapper, stripped, username, mapping.upstreamModelId(),
                                    toolsEnabled, streamingEnabled);
                        } catch (com.fasterxml.jackson.core.JsonProcessingException e) {
                            log.warn("Bedrock body metadata normalize failed, using stripped body: {}", e.getMessage());
                            bodyForBedrock = stripped;
                        }

                        if (!supportsStreaming(mapping)) {
                            invokeNonStreaming(client, mapping, bodyForBedrock, username, model,
                                    useAnthropic, converterRef, emitter, invokeFutureRef, streamCompleted,
                                    correlationId);
                            return;
                        }

                        logToolRequestOutbound(correlationId, username, model, mapping.upstreamModelId(), bodyForBedrock);

                        // 构建 Bedrock 流式调用请求
                        var request = InvokeModelWithResponseStreamRequest.builder()
                                .modelId(mapping.upstreamModelId())
                                .contentType("application/json")
                                .accept("application/json")
                                .body(SdkBytes.fromString(bodyForBedrock, StandardCharsets.UTF_8))
                                .build();

                        // 响应流访问器：处理每个 chunk
                        var visitor = InvokeModelWithResponseStreamResponseHandler.Visitor.builder()
                                .onChunk(payload -> {
                                    String data = payload.bytes().asUtf8String();
                                    accumulateUsage(data, inputTokens, outputTokens);
                                    int chunkIdx = rawChunkSeq.getAndIncrement();

                                    try {
                                        if (useAnthropic) {
                                            var converter = converterRef.updateAndGet(
                                                    c -> c != null ? c : new AnthropicSseConverter(objectMapper, model,
                                                            correlationId, props.toolCallTraceEnabled(),
                                                            props.toolCallTraceMaxChars()));
                                            List<ServerSentEvent<String>> batch = new ArrayList<>();
                                            for (var event : converter.convert(data)) {
                                                emitter.next(event);
                                                batch.add(event);
                                            }
                                            logToolBedrockChunkIfNeeded(correlationId, chunkIdx, data);
                                            logToolAnthropicUpstreamBatch(correlationId, chunkIdx, batch);
                                        } else {
                                            emitter.next(ServerSentEvent.<String>builder()
                                                    .data(data).build());
                                            if (props.toolCallTraceEnabled() && data.contains("\"tool_calls\"")) {
                                                toolTrace.info("ToolTrace RAW_BEDROCK_CHUNK traceId={} chunkIdx={} bytes={} preview={}",
                                                        correlationId, chunkIdx, data.length(),
                                                        truncateToolLog(data, props.toolCallTraceMaxChars()));
                                            }
                                        }
                                    } catch (Exception e) {
                                        log.error("Failed to emit SSE event for traceId={} user={}", correlationId, username, e);
                                        emitter.error(e);
                                    }
                                })
                                .build();

                        // 构建响应流处理器
                        var handler = InvokeModelWithResponseStreamResponseHandler.builder()
                                .onEventStream(eventStream -> {
                                    eventStream.subscribe(new Subscriber<ResponseStream>() {
                                        @Override
                                        public void onSubscribe(Subscription s) {
                                            s.request(Long.MAX_VALUE);
                                        }

                                        @Override
                                        public void onNext(ResponseStream event) {
                                            event.accept(visitor);
                                        }

                                        @Override
                                        public void onError(Throwable t) {
                                            log.error("Bedrock stream error for user={}", username, t);
                                            emitter.error(t);
                                        }

                                        @Override
                                        public void onComplete() {
                                        }
                                    });
                                })
                                .onError(e -> {
                                    log.error("Bedrock invoke error for user={}", username, e);
                                    emitter.error(e);
                                })
                                .onComplete(() -> {
                                    streamCompleted.set(true);
                                    log.info("Bedrock stream completed for user={} model={}", username, model);
                                    // 部分上游在流末尾不携带 finish_reason，需在关流前补发 Anthropic 结束帧
                                    if (useAnthropic) {
                                        AnthropicSseConverter c = converterRef.get();
                                        if (c != null) {
                                            try {
                                                for (var event : c.endStreamIfOpen()) {
                                                    emitter.next(event);
                                                }
                                            } catch (Exception e) {
                                                log.error("Failed to emit synthetic Anthropic tail for user={}", username, e);
                                                emitter.error(e);
                                                return;
                                            }
                                            if (props.toolCallTraceEnabled()) {
                                                String tsum = c.summarizeToolConversionAtStreamEnd();
                                                toolTrace.info("ToolTrace STREAM_DONE traceId={} user={} userModel={} bedrockModelId={} rawChunks={} lifecycle={} syntheticInitialTail={} inputTok={} outputTok={} tools={}",
                                                        correlationId, username, model, mapping.upstreamModelId(),
                                                        rawChunkSeq.get(), c.debugLifecycleState(), c.isSyntheticInitialTailUsed(),
                                                        c.getInputTokens(), c.getOutputTokens(),
                                                        tsum.isEmpty() ? "(no_tool_buffer)" : tsum);
                                            }
                                        }
                                    }
                                    emitter.complete();
                                })
                                .build();

                        CompletableFuture<Void> invokeFuture = client.invokeModelWithResponseStream(request, handler);
                        invokeFutureRef.set(invokeFuture);
                        invokeFuture.whenComplete((ignored, throwable) -> {
                            if (throwable != null) {
                                log.warn("Bedrock invoke future completed exceptionally for user={} model={}: {}",
                                        username, model, throwable.toString());
                            }
                        });

                        // 注册取消回调：客户端断开连接时取消 Bedrock 请求
                        emitter.onDispose(() -> {
                            if (streamCompleted.get()) {
                                return;
                            }
                            CompletableFuture<Void> f = invokeFutureRef.get();
                            if (f != null && !f.isDone()) {
                                boolean cancelled = f.cancel(true);
                                log.debug("Disposed Bedrock stream for user={} model={} cancelled={}", username, model, cancelled);
                            }
                        });
                    } catch (Exception e) {
                        log.error("Bedrock invoke error for user={}", username, e);
                        emitter.error(e);
                    }
                })
                .doFinally(signal -> {
                    if (signal == SignalType.CANCEL && !streamCompleted.get()) {
                        if (props.toolCallTraceEnabled()) {
                            toolTrace.info("ToolTrace CLIENT_CANCEL traceId={} user={} model={} rawChunksSoFar={}",
                                    correlationId, username, model, rawChunkSeq.get());
                        }
                        log.debug("Client disconnected before completion for user={} model={}", username, model);
                    }
                });
    }

    /** 对不稳定或未声明流式能力的模型使用非流式 InvokeModel，并包装为 SSE 返回给上游。 */
    private void invokeNonStreaming(BedrockRuntimeAsyncClient client, ProviderConfig mapping, String bodyForBedrock,
                                    String username, String model, boolean useAnthropic,
                                    AtomicReference<AnthropicSseConverter> converterRef,
                                    reactor.core.publisher.FluxSink<ServerSentEvent<String>> emitter,
                                    AtomicReference<CompletableFuture<Void>> invokeFutureRef,
                                    AtomicBoolean streamCompleted,
                                    String traceId) {
        var request = InvokeModelRequest.builder()
                .modelId(mapping.upstreamModelId())
                .contentType("application/json")
                .accept("application/json")
                .body(SdkBytes.fromString(bodyForBedrock, StandardCharsets.UTF_8))
                .build();

        logToolRequestOutbound(traceId, username, model, mapping.upstreamModelId(), bodyForBedrock);

        CompletableFuture<Void> future = client.invokeModel(request)
                .thenAccept(response -> {
                    String data = response.body().asUtf8String();
                    try {
                        if (useAnthropic) {
                            var converter = converterRef.updateAndGet(
                                    c -> c != null ? c : new AnthropicSseConverter(objectMapper, model, traceId,
                                            props.toolCallTraceEnabled(), props.toolCallTraceMaxChars()));
                            for (var event : converter.convertCompleteMessage(data)) {
                                emitter.next(event);
                            }
                            if (props.toolCallTraceEnabled()) {
                                if (data.contains("\"tool_calls\"")) {
                                    toolTrace.info("ToolTrace RAW_NON_STREAM_RESPONSE traceId={} user={} bytes={} preview={}",
                                            traceId, username, data.length(),
                                            truncateToolLog(data, props.toolCallTraceMaxChars()));
                                }
                                var conv = converterRef.get();
                                if (conv != null) {
                                    String tsum = conv.summarizeToolConversionAtStreamEnd();
                                    if (data.contains("\"tool_calls\"") || !tsum.isEmpty()) {
                                        toolTrace.info("ToolTrace NON_STREAM_DONE traceId={} user={} userModel={} bedrockModelId={} lifecycle={} syntheticInitialTail={} inputTok={} outputTok={} tools={}",
                                                traceId, username, model, mapping.upstreamModelId(),
                                                conv.debugLifecycleState(), conv.isSyntheticInitialTailUsed(),
                                                conv.getInputTokens(), conv.getOutputTokens(),
                                                tsum.isEmpty() ? "(no_tool_buffer)" : tsum);
                                    }
                                }
                            }
                        } else {
                            emitter.next(ServerSentEvent.<String>builder().data(data).build());
                            if (props.toolCallTraceEnabled() && data.contains("\"tool_calls\"")) {
                                toolTrace.info("ToolTrace RAW_NON_STREAM_RESPONSE traceId={} bytes={} preview={}",
                                        traceId, data.length(), truncateToolLog(data, props.toolCallTraceMaxChars()));
                            }
                        }
                        streamCompleted.set(true);
                        log.info("Bedrock non-stream completed for user={} model={}", username, model);
                        emitter.complete();
                    } catch (Exception e) {
                        log.error("Failed to emit non-stream SSE response for user={}", username, e);
                        emitter.error(e);
                    }
                });
        invokeFutureRef.set(future);
        future.whenComplete((ignored, throwable) -> {
            if (throwable != null && !future.isCancelled()) {
                log.warn("Bedrock non-stream future completed exceptionally for user={} model={}: {}",
                        username, model, throwable.toString());
                emitter.error(throwable);
            }
        });
    }

    /** 发往 Bedrock 的请求里与工具相关的摘要（不写整包 body）。 */
    private void logToolRequestOutbound(String traceId, String user, String userModel, String bedrockModelId, String bodyJson) {
        if (!props.toolCallTraceEnabled()) {
            return;
        }
        try {
            JsonNode root = objectMapper.readTree(bodyJson);
            JsonNode tools = root.get("tools");
            JsonNode tc = root.get("tool_choice");
            String toolChoice = tc == null || tc.isNull() ? "none" : truncateToolLog(tc.toString(), props.toolCallTraceMaxChars());
            String toolsSummary = summarizeToolsForTrace(tools);
            toolTrace.info("ToolTrace REQUEST_OUT traceId={} user={} userModel={} bedrockModelId={} bodyChars={} tool_choice={} tools={}",
                    traceId, user, userModel, bedrockModelId, bodyJson.length(), toolChoice, toolsSummary);
        } catch (Exception e) {
            toolTrace.warn("ToolTrace REQUEST_OUT traceId={} parse_failed={}", traceId, e.getMessage());
        }
    }

    private static String summarizeToolsForTrace(JsonNode tools) {
        if (tools == null || !tools.isArray() || tools.isEmpty()) {
            return "none";
        }
        StringBuilder sb = new StringBuilder();
        for (JsonNode t : tools) {
            if (!t.isObject()) {
                continue;
            }
            String type = t.path("type").asText("?");
            String name = t.path("name").asText("");
            if (name.isEmpty() && t.has("function")) {
                name = t.path("function").path("name").asText("");
            }
            if (sb.length() > 0) {
                sb.append(';');
            }
            sb.append(type).append(':').append(name.isEmpty() ? "?" : name);
        }
        return sb.toString();
    }

    private void logToolBedrockChunkIfNeeded(String traceId, int chunkIdx, String rawUtf8) {
        if (!props.toolCallTraceEnabled() || rawUtf8 == null || !rawUtf8.contains("\"tool_calls\"")) {
            return;
        }
        toolTrace.info("ToolTrace RAW_BEDROCK_CHUNK traceId={} chunkIdx={} bytes={} preview={}",
                traceId, chunkIdx, rawUtf8.length(), truncateToolLog(rawUtf8, props.toolCallTraceMaxChars()));
    }

    private void logToolAnthropicUpstreamBatch(String traceId, int chunkIdx, List<ServerSentEvent<String>> batch) {
        if (!props.toolCallTraceEnabled() || batch == null || batch.isEmpty()) {
            return;
        }
        for (ServerSentEvent<String> ev : batch) {
            if (!isAnthropicUpstreamToolSse(ev)) {
                continue;
            }
            String d = ev.data();
            toolTrace.info("ToolTrace UPSTREAM_SSE traceId={} chunkIdx={} event={} dataPreview={}",
                    traceId, chunkIdx, ev.event(), truncateToolLog(d != null ? d : "", props.toolCallTraceMaxChars()));
        }
    }

    private static boolean isAnthropicUpstreamToolSse(ServerSentEvent<String> ev) {
        if (ev == null) {
            return false;
        }
        String name = ev.event();
        if (name == null) {
            return false;
        }
        if (!"content_block_start".equals(name) && !"content_block_delta".equals(name)) {
            return false;
        }
        String d = ev.data();
        if (d == null) {
            return false;
        }
        return d.contains("\"tool_use\"") || d.contains("input_json_delta") || d.contains("partial_json");
    }

    private static String truncateToolLog(String s, int maxChars) {
        if (s == null) {
            return "";
        }
        String one = s.replace('\r', ' ').replace('\n', ' ');
        if (one.length() <= maxChars) {
            return one;
        }
        return one.substring(0, maxChars) + "...(truncated,totalChars=" + s.length() + ")";
    }

    /** 根据模型能力声明判断是否允许向下游发送工具定义和工具调用历史。 */
    private boolean supportsTools(ProviderConfig mapping) {
        if (mapping.capabilities() == null) {
            return false;
        }
        return mapping.capabilities().stream()
                .filter(c -> c != null)
                .map(c -> c.toLowerCase(Locale.ROOT))
                .anyMatch(c -> c.equals("tool") || c.equals("tools") || c.equals("function_calling"));
    }

    /** 只有显式声明 stream/streaming 的模型走 InvokeModelWithResponseStream。 */
    private boolean supportsStreaming(ProviderConfig mapping) {
        if (mapping.capabilities() == null) {
            return false;
        }
        return mapping.capabilities().stream()
                .filter(c -> c != null)
                .map(c -> c.toLowerCase(Locale.ROOT))
                .anyMatch(c -> c.equals("stream") || c.equals("streaming"));
    }

    /**
     * 从请求体 JSON 中移除 model 字段，避免 Bedrock 因不识别的字段而拒绝请求。
     * model 已通过 {@code InvokeModelRequest.modelId()} 单独指定。
     */
    private String stripModelField(String body) {
        int idx = body.indexOf("\"model\"");
        if (idx < 0) return body;
        // 找到 model 字段值结束的位置
        int colon = body.indexOf(":", idx);
        if (colon < 0) return body;
        int startQuote = body.indexOf("\"", colon + 1);
        if (startQuote < 0) return body;
        int endQuote = body.indexOf("\"", startQuote + 1);
        if (endQuote < 0) return body;
        // 向前跳过逗号，向后跳过字段结尾的逗号
        int start = idx;
        while (start > 0 && (body.charAt(start - 1) == ' ' || body.charAt(start - 1) == '\t')) start--;
        // 检查 model 后面的逗号，将其一并移除
        int afterEnd = endQuote + 1;
        while (afterEnd < body.length() && (body.charAt(afterEnd) == ' ' || body.charAt(afterEnd) == '\t')) afterEnd++;
        if (afterEnd < body.length() && body.charAt(afterEnd) == ',') {
            afterEnd++; // 吃掉逗号
        } else if (start > 0 && body.charAt(start - 1) == ',') {
            // model 后面没有逗号，但前面有逗号（说明 model 不是第一个字段），清除前面的逗号
            start--;
        }
        while (afterEnd < body.length() && body.charAt(afterEnd) == ' ') afterEnd++;
        return body.substring(0, start) + body.substring(afterEnd);
    }

    /**
     * 从 Bedrock 响应 JSON 中提取 token 用量并累加到原子计数器
     */
    private void accumulateUsage(String data, AtomicInteger inputTokens, AtomicInteger outputTokens) {
        if (data == null) {
            return;
        }
        extractIntField(data, "input_tokens").ifPresent(inputTokens::set);
        extractIntField(data, "output_tokens").ifPresent(outputTokens::set);
        // Bedrock 部分模型在流末 chunk 附带 amazon-bedrock-invocationMetrics（驼峰字段）
        extractIntField(data, "inputTokenCount").ifPresent(inputTokens::set);
        extractIntField(data, "outputTokenCount").ifPresent(outputTokens::set);
    }

    /**
     * 从 JSON 字符串中简单提取指定字段的整数值（不依赖完整 JSON 解析，避免性能开销）
     *
     * @param json  原始 JSON 字符串
     * @param field 要提取的字段名
     * @return 提取到的整数值 Optional
     */
    private java.util.Optional<Integer> extractIntField(String json, String field) {
        try {
            // 查找字段名
            int idx = json.indexOf("\"" + field + "\"");
            if (idx < 0) return java.util.Optional.empty();
            // 查找冒号
            int colon = json.indexOf(":", idx);
            if (colon < 0) return java.util.Optional.empty();
            // 提取冒号后的数值
            String rest = json.substring(colon + 1).trim();
            int end = 0;
            while (end < rest.length() && (Character.isDigit(rest.charAt(end)) || rest.charAt(end) == '-')) {
                end++;
            }
            if (end > 0) {
                return java.util.Optional.of(Integer.parseInt(rest.substring(0, end)));
            }
        } catch (Exception e) {
            // 解析失败时静默忽略
        }
        return java.util.Optional.empty();
    }
}
