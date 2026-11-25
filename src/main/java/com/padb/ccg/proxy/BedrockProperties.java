package com.padb.ccg.proxy;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Bedrock 配置属性，绑定 {@code bedrock.*} 配置项。
 * 超时与重试见第一层级 {@link UpstreamProperties}（{@code upstream.*}）。
 *
 * <p>上游请求与响应的协议形态由模型族决定（见
 * {@link BedrockModelFamily#of(String)}），不再由配置项控制。</p>
 *
 * @param region             默认 AWS 区域
 * @param accessKey          AWS 访问密钥
 * @param secretKey          AWS 秘密密钥
 * @param sessionToken       STS 会话令牌（STS 临时凭证必须，ASIA 开头密钥必需）
 * @param toolCallTraceEnabled 为 true 时仅输出工具调用链排查日志（发往 Bedrock 的工具列表、含 tool_calls 的 chunk、
 *                             转换出的 Anthropic tool_use / input_json_delta），不写整包 body
 * @param toolCallTraceMaxChars 工具排查日志中单段预览的最大字符数
 */
@ConfigurationProperties(prefix = "bedrock")
public record BedrockProperties(String region, String accessKey, String secretKey,
                                 String sessionToken,
                                 boolean toolCallTraceEnabled,
                                 int toolCallTraceMaxChars) {
    public BedrockProperties {
        if (region == null || region.isBlank()) region = "us-east-1";
        if (toolCallTraceMaxChars <= 0) {
            toolCallTraceMaxChars = 4096;
        }
    }
}
