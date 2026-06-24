package com.padb.ccg.core.model;

/**
 * 大模型上游渠道：按模型映射配置决定请求转发到 AWS Bedrock 或华为云 MaaS。
 */
public enum ProviderChannel {
    /** AWS Bedrock Runtime */
    AWS,
    /** 华为云 MaaS Anthropic 兼容接口 */
    HUAWEI
}
