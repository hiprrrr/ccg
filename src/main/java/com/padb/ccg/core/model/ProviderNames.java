package com.padb.ccg.core.model;

/**
 * 供应商标识常量。{@code aws} 走 Bedrock；其余名称须在 {@code other-providers} 列表中声明。
 */
public final class ProviderNames {

    /** AWS Bedrock 渠道在 model-mappings / 日志中的标识 */
    public static final String AWS = "aws";

    private ProviderNames() {
    }

    /** 判断是否为 AWS Bedrock 渠道（大小写不敏感） */
    public static boolean isAws(String provider) {
        return provider != null && AWS.equalsIgnoreCase(provider.trim());
    }
}
