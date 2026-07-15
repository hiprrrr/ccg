package com.padb.ccg.proxy;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.ArrayList;

/**
 * HTTP 透传供应商列表，绑定 {@code other-providers}（仅供应商条目，不含超时/重试）。
 */
@ConfigurationProperties(prefix = "other-providers")
public class OtherProvidersProperties extends ArrayList<OtherProviderItem> {
}
