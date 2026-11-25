package com.padb.ccg.proxy;

import com.padb.ccg.core.exception.ProviderException;
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
 * 1. 按区域缓存 {@link BedrockRuntimeAsyncClient} 实例（底层 Netty 读超时与 {@code upstream.timeout-seconds} 对齐，避免流式 chunk 间隔过长被默认 ~30s 读超时断开）
 * 2. 构建流式调用请求，通过 Flux.push 桥接 Bedrock 异步回调到 Reactor Flux
 * 3. 实时提取响应中的 token 用量信息
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
    private final UpstreamProperties upstreamProperties;
    private final ObjectMapper objectMapper;

    /** 按区域缓存 BedrockAsyncClient，避免重复创建 */
    private final Map<String, BedrockRuntimeAsyncClient> clients = new ConcurrentHashMap<>();

    public BedrockProxyHandler(BedrockProperties props, UpstreamProperties upstreamProperties,
                               ObjectMapper objectMapper) {
        this.props = props;
        this.upstreamProperties = upstreamProperties;
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
     * <p>默认 Netty 客户端两次读事件间隔约 30s 即超时；大模型首包或 chunk 间隔可能超过该值，需与 {@link UpstreamProperties#timeoutSeconds()} 一致。</p>
     */
    private BedrockRuntimeAsyncClient buildBedrockClientForRegion(String awsRegion) {
        int sec = Math.max(1, upstreamProperties.timeoutSeconds());
        Duration d = Duration.ofSeconds(sec);
        SdkAsyncHttpClient http = NettyNioAsyncHttpClient.builder()
                .readTimeout(d)
                .writeTimeout(d)
                .connectionTimeout(Duration.ofSeconds(30))
                .build();
        return BedrockRuntimeAsyncClient.builder()
                .region(Region.of(awsRegion))
                .credentialsProvider(bedrockCredentialsProvider())
                .httpClient(http)
                .overrideConfiguration(b -> b
                        .retryPolicy(RetryPolicy.builder().numRetries(upstreamProperties.retryMax()).build())
                        .apiCallTimeout(d))
                .build();
    }

    /**
     * Bedrock 客户端使用的凭证提供者。
     * <p>若未配置 {@code bedrock.access-key}，则走 {@link DefaultCredentialsProvider}（实例元数据 / WebIdentity 等，SDK 会按链自动刷新）。</p>
     * <p>若配置了静态密钥，则<strong>每次</strong> {@link AwsCredentialsProvider#resolveCredentials()} 时从 {@link BedrockProperties} 读取，
     * 避免 {@code StaticCredentialsProvider} 把 STS 会话凭证冻结在「首次按 region 建连」时刻；配置中心或 RefreshScope 更新密钥后无需重启进程
     * （仍需在过期前推送新 token；若从不刷新配置，临时凭证过期后只能换密钥或改用默认链）。</p>
     */
    private AwsCredentialsProvider bedrockCredentialsProvider() {
        String ak = props.accessKey();
        if (ak == null || ak.isBlank()) {
            return DEFAULT_CREDENTIALS_PROVIDER;
        }
        return this::resolveCredentials;
    }

    /**
     * 根据配置解析 AWS 凭证：有 session token 时使用 STS 会话凭证，否则使用基本凭证（由 {@link #bedrockCredentialsProvider()} 按需调用）
     */
    private software.amazon.awssdk.auth.credentials.AwsCredentials resolveCredentials() {
        String token = props.sessionToken();
        if (token != null && !token.isBlank()) {
            return AwsSessionCredentials.create(props.accessKey(), props.secretKey(), token);
        }
        return AwsBasicCredentials.create(props.accessKey(), props.secretKey());
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

        // 确定使用的 AWS 区域：优先使用映射配置，否则用默认配置
        String region = mapping.region() != null ? mapping.region() : props.region();

        // 上游协议家族：显式配置优先，否则按 upstream-model-id 推断。
        // ANTHROPIC → 请求保持 Anthropic 形态、响应为 Anthropic 事件；
        // OPENAI_CHAT → 请求转 OpenAI Chat 形态、响应为 OpenAI chunk；
        // NOVA → 请求转 Nova 原生 schema、响应为 Nova 事件。
        final BedrockModelFamily family = BedrockModelFamily.of(mapping.upstreamModelId(), mapping.upstreamFormat());
        if (!family.isSupported()) {
            String supported = String.join("/", BedrockModelFamily.supportedFormatNames());
            log.warn("Unsupported Bedrock model family: model={} upstreamModelId={} upstreamFormat={}",
                    model, mapping.upstreamModelId(), mapping.upstreamFormat());
            return Flux.error(new ProviderException("Bedrock model '" + mapping.upstreamModelId()
                    + "' belongs to an unsupported protocol family — declare upstream-format ("
                    + supported + ") in model-mappings to override the automatic detection"));
        }
        boolean upstreamOpenAiChat = family == BedrockModelFamily.OPENAI_CHAT;
        AtomicReference<AnthropicSseConverter> converterRef = new AtomicReference<>();
        AtomicReference<NovaSseConverter> novaConverterRef = new AtomicReference<>();
        AtomicReference<CompletableFuture<Void>> invokeFutureRef = new AtomicReference<>();
        AtomicBoolean streamCompleted = new AtomicBoolean(false);
        /** 与 ProxyService 入口日志共用的关联 ID，便于把 Bedrock 原始 chunk 与一次 HTTP 请求对齐 */
        final String correlationId = (traceId == null || traceId.isBlank()) ? "-" : traceId;
        /** Bedrock SDK onChunk 收到的原始 UTF-8 块序号（从 0 递增） */
        final AtomicInteger rawChunkSeq = new AtomicInteger(0);

        // 使用 Flux.push 桥接 Bedrock 异步回调到 Reactor Flux
        // Flux.push 内部处理跨线程 emit 的序列化，比 Sinks.Many 更适合回调场景
        return Flux.<ServerSentEvent<String>>push(emitter -> {
                    // 按区域获取或创建 BedrockAsyncClient
                    BedrockRuntimeAsyncClient client = clients.computeIfAbsent(region,
                            k -> buildBedrockClientForRegion(k));

                    try {
                        // 规范化请求体：移除 model 字段并重写 metadata，避免上游 user_id 等违反 Bedrock 正则导致 400
                        String bodyForBedrock;
                        try {
                            boolean toolsEnabled = supportsTools(mapping);
                            boolean streamingEnabled = supportsStreaming(mapping);
                            if (family == BedrockModelFamily.NOVA) {
                                // Nova 原生 schema：不接受 model / metadata 等 Anthropic 字段
                                bodyForBedrock = BedrockNovaBodyPreparer.toNovaBody(objectMapper, requestBody,
                                        toolsEnabled);
                            } else {
                                bodyForBedrock = BedrockInvokeBodyPreparer.normalizeMetadata(
                                        objectMapper, requestBody, username, mapping.upstreamModelId(),
                                        toolsEnabled, streamingEnabled, family);
                            }
                        } catch (com.fasterxml.jackson.core.JsonProcessingException e) {
                            log.warn("Bedrock body metadata normalize failed, using original body: {}", e.getMessage());
                            bodyForBedrock = requestBody;
                        }

                        if (!supportsStreaming(mapping)) {
                            invokeNonStreaming(client, mapping, bodyForBedrock, username, model,
                                    family, upstreamOpenAiChat, converterRef, emitter, invokeFutureRef,
                                    streamCompleted, inputTokens, outputTokens, correlationId);
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
                                        if (upstreamOpenAiChat) {
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
                                        } else if (family == BedrockModelFamily.NOVA) {
                                            // Nova 事件 → Anthropic SSE
                                            var converter = novaConverterRef.updateAndGet(
                                                    c -> c != null ? c : new NovaSseConverter(objectMapper, model));
                                            List<ServerSentEvent<String>> batch = new ArrayList<>();
                                            for (var event : converter.convert(data)) {
                                                emitter.next(event);
                                                batch.add(event);
                                            }
                                            logToolAnthropicUpstreamBatch(correlationId, chunkIdx, batch);
                                        } else {
                                            // 上游已是 Anthropic 事件：按事件类型还原 SSE event 名后直传
                                            ServerSentEvent<String> event = toAnthropicSseEvent(data);
                                            if (event != null) {
                                                emitter.next(event);
                                                logToolAnthropicUpstreamBatch(correlationId, chunkIdx,
                                                        List.of(event));
                                            }
                                            logToolBedrockChunkIfNeeded(correlationId, chunkIdx, data);
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
                                    // （仅 OpenAI Chat 形态上游需要：Anthropic 族上游自带 message_stop）
                                    if (upstreamOpenAiChat) {
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
                                    } else if (family == BedrockModelFamily.NOVA) {
                                        // Nova 上游可能不发 messageStop，关流前补齐 Anthropic 结束事件
                                        NovaSseConverter c = novaConverterRef.get();
                                        if (c != null) {
                                            for (var event : c.endStreamIfOpen()) {
                                                emitter.next(event);
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

    /**
     * 对不稳定或未声明流式能力的模型使用非流式 InvokeModel，并包装为 SSE 返回给上游。
     * 包级可见以便单测直接构造调用（验证非流式响应的 token 计账）。
     */
    void invokeNonStreaming(BedrockRuntimeAsyncClient client, ProviderConfig mapping, String bodyForBedrock,
                                    String username, String model, BedrockModelFamily family,
                                    boolean upstreamOpenAiChat,
                                    AtomicReference<AnthropicSseConverter> converterRef,
                                    reactor.core.publisher.FluxSink<ServerSentEvent<String>> emitter,
                                    AtomicReference<CompletableFuture<Void>> invokeFutureRef,
                                    AtomicBoolean streamCompleted,
                                    AtomicInteger inputTokens, AtomicInteger outputTokens,
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
                    // 非流式响应同样要计账 token 用量（此前漏掉导致请求日志恒为 0）
                    accumulateUsage(data, inputTokens, outputTokens);
                    try {
                        if (upstreamOpenAiChat) {
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
                        } else if (family == BedrockModelFamily.NOVA) {
                            // Nova 非流式响应 → Anthropic message → Anthropic SSE 事件序列
                            String anthropicJson = NovaSseConverter.toAnthropicMessage(objectMapper, data, model);
                            for (var event : AnthropicMessageSseEvents.fromMessage(objectMapper, anthropicJson, model)) {
                                emitter.next(event);
                            }
                            if (props.toolCallTraceEnabled()) {
                                toolTrace.info("ToolTrace RAW_NON_STREAM_RESPONSE traceId={} user={} bytes={} preview={}",
                                        traceId, username, data.length(),
                                        truncateToolLog(data, props.toolCallTraceMaxChars()));
                            }
                        } else {
                            // 上游已是 Anthropic message JSON：展开为 Anthropic SSE 事件序列
                            for (var event : AnthropicMessageSseEvents.fromMessage(objectMapper, data, model)) {
                                emitter.next(event);
                            }
                            if (props.toolCallTraceEnabled()) {
                                toolTrace.info("ToolTrace RAW_NON_STREAM_RESPONSE traceId={} user={} bytes={} preview={}",
                                        traceId, username, data.length(),
                                        truncateToolLog(data, props.toolCallTraceMaxChars()));
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

    /**
     * 将上游返回的 Anthropic 事件 chunk 还原为带 event 名的 Anthropic SSE 事件。
     * <p>Anthropic 族模型（{@code anthropic.claude-*} 等）在 Bedrock 上直接返回 Anthropic 事件，
     * 但 SDK 只给出事件体，事件名需按 body 中的 {@code type} 字段还原，
     * 否则下游 Claude Code 收不到 {@code event: content_block_delta} 而中断。</p>
     * <p>非事件 chunk（如 {@code amazon-bedrock-invocationMetrics}）返回 {@code null}，不向下游发送。</p>
     */
    ServerSentEvent<String> toAnthropicSseEvent(String rawChunk) {
        if (rawChunk == null || rawChunk.isBlank()) {
            return null;
        }
        final JsonNode root;
        try {
            root = objectMapper.readTree(rawChunk);
        } catch (Exception e) {
            log.warn("Failed to parse Bedrock chunk as Anthropic event: {}", e.getMessage());
            return null;
        }
        String type = root.path("type").asText("");
        if (type.isEmpty()) {
            return null;
        }
        return ServerSentEvent.<String>builder().event(type).data(rawChunk).build();
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
        // Nova 原生 schema 的 usage 字段为驼峰（inputTokens / outputTokens）
        extractIntField(data, "inputTokens").ifPresent(inputTokens::set);
        extractIntField(data, "outputTokens").ifPresent(outputTokens::set);
        // OpenAI Chat 形态的非流式响应 usage 为 prompt_tokens / completion_tokens
        extractIntField(data, "prompt_tokens").ifPresent(inputTokens::set);
        extractIntField(data, "completion_tokens").ifPresent(outputTokens::set);
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
