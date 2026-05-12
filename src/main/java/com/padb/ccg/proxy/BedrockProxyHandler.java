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
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.core.scheduler.Schedulers;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.core.retry.RetryPolicy;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.bedrockruntime.BedrockRuntimeAsyncClient;
import software.amazon.awssdk.services.bedrockruntime.model.InvokeModelWithResponseStreamRequest;
import software.amazon.awssdk.services.bedrockruntime.model.InvokeModelWithResponseStreamResponseHandler;
import software.amazon.awssdk.services.bedrockruntime.model.ResponseStream;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Bedrock 代理处理器，负责将请求转发到 AWS Bedrock 并通过 SSE 流式返回响应。
 *
 * 核心流程：
 * 1. 按区域缓存 {@link BedrockRuntimeAsyncClient} 实例
 * 2. 构建流式调用请求，通过 {@link Sinks.Many} 桥接 Bedrock 异步回调到 Reactor Flux
 * 3. 在 boundedElastic 调度器上执行阻塞性 AWS SDK 调用
 * 4. 实时提取响应中的 token 用量信息
 */
@Component
public class BedrockProxyHandler {

    private static final Logger log = LoggerFactory.getLogger(BedrockProxyHandler.class);

    private final BedrockProperties props;

    /** 按区域缓存 BedrockAsyncClient，避免重复创建 */
    private final Map<String, BedrockRuntimeAsyncClient> clients = new ConcurrentHashMap<>();

    public BedrockProxyHandler(BedrockProperties props) {
        this.props = props;
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
     * 转发请求到 Bedrock 并返回 SSE 流
     *
     * @param mapping      模型映射配置
     * @param requestBody  请求体 JSON
     * @param inputTokens  输入 token 累加器（原子更新）
     * @param outputTokens 输出 token 累加器（原子更新）
     * @param username     用户名
     * @param model        用户请求的模型名称
     * @return SSE 事件流
     */
    public Flux<ServerSentEvent<String>> forward(ProviderConfig mapping, String requestBody,
                                                  AtomicInteger inputTokens, AtomicInteger outputTokens,
                                                  String username, String model) {

        // 确定使用的 AWS 区域：优先使用映射配置，否则用默认配置
        String region = mapping.region() != null ? mapping.region() : props.region();

        // 创建背压缓冲 Sink，用于将 Bedrock 回调事件桥接到 Reactor Flux
        Sinks.Many<ServerSentEvent<String>> sink = Sinks.many().unicast().onBackpressureBuffer();

        // 在 boundedElastic 线程池执行 AWS SDK 阻塞调用
        Mono.fromRunnable(() -> {
                    // 按区域获取或创建 BedrockAsyncClient
                    BedrockRuntimeAsyncClient client = clients.computeIfAbsent(region,
                            k -> BedrockRuntimeAsyncClient.builder()
                                    .region(Region.of(k))
                                    .credentialsProvider(StaticCredentialsProvider.create(
                                            AwsBasicCredentials.create(props.accessKey(), props.secretKey())))
                                    .overrideConfiguration(b -> b
                                            .retryPolicy(RetryPolicy.builder().numRetries(props.retryMax()).build())
                                            .apiCallTimeout(
                                                    java.time.Duration.ofSeconds(props.timeoutSeconds())))
                                    .build());

                    try {
                        // 构建 Bedrock 流式调用请求
                        var request = InvokeModelWithResponseStreamRequest.builder()
                                .modelId(mapping.bedrockModelId())
                                .contentType("application/json")
                                .accept("application/json")
                                .body(SdkBytes.fromString(requestBody, StandardCharsets.UTF_8))
                                .build();

                        // 响应流访问器：处理每个 chunk
                        var visitor = InvokeModelWithResponseStreamResponseHandler.Visitor.builder()
                                .onChunk(payload -> {
                                    String data = payload.bytes().asUtf8String();
                                    // 从响应数据中提取 token 用量
                                    accumulateUsage(data, inputTokens, outputTokens);
                                    // 将数据包装为 SSE 事件发送
                                    sink.tryEmitNext(ServerSentEvent.<String>builder()
                                            .data(data).build());
                                })
                                .build();

                        // 构建响应流处理器
                        var handler = InvokeModelWithResponseStreamResponseHandler.builder()
                                .onEventStream(eventStream -> {
                                    // 订阅 Bedrock 事件流
                                    eventStream.subscribe(new Subscriber<ResponseStream>() {
                                        @Override
                                        public void onSubscribe(Subscription s) {
                                            // 请求无限数据
                                            s.request(Long.MAX_VALUE);
                                        }

                                        @Override
                                        public void onNext(ResponseStream event) {
                                            event.accept(visitor);
                                        }

                                        @Override
                                        public void onError(Throwable t) {
                                            log.error("Bedrock stream error for user={}", username, t);
                                            sink.tryEmitError(t);
                                        }

                                        @Override
                                        public void onComplete() {
                                            // 流完成由外层 onComplete 处理
                                        }
                                    });
                                })
                                .onError(e -> {
                                    log.error("Bedrock invoke error for user={}", username, e);
                                    sink.tryEmitError(e);
                                })
                                .onComplete(() -> {
                                    log.info("Bedrock stream completed for user={} model={}", username, model);
                                    sink.tryEmitComplete();
                                })
                                .build();

                        // 发起流式调用
                        client.invokeModelWithResponseStream(request, handler);
                    } catch (Exception e) {
                        log.error("Bedrock invoke error for user={}", username, e);
                        sink.tryEmitError(e);
                    }
                })
                .subscribeOn(Schedulers.boundedElastic())
                .subscribe(
                        v -> {},
                        e -> log.error("Unexpected error in Bedrock proxy for user={}", username, e)
                );

        return sink.asFlux()
                .doOnCancel(() -> log.info("Client disconnected for user={}", username));
    }

    /**
     * 从 Bedrock 响应 JSON 中提取 token 用量并累加到原子计数器
     */
    private void accumulateUsage(String data, AtomicInteger inputTokens, AtomicInteger outputTokens) {
        if (data == null) return;
        extractIntField(data, "input_tokens").ifPresent(inputTokens::set);
        extractIntField(data, "output_tokens").ifPresent(outputTokens::set);
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
