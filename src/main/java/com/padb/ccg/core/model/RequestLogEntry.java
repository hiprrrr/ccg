package com.padb.ccg.core.model;

import java.time.Instant;

/**
 * 请求日志条目，记录每次 API 代理请求的详细信息，用于审计和监控
 *
 * @param username     请求用户名
 * @param model        请求的模型名称
 * @param providerId   实际调用的供应商模型 ID
 * @param success      请求是否成功
 * @param errorMsg     错误信息（成功时为 null）
 * @param inputTokens  输入 token 数量
 * @param outputTokens 输出 token 数量
 * @param durationMs   请求耗时（毫秒）
 * @param createdAt    日志创建时间
 */
public record RequestLogEntry(String username, String model, String providerId,
                              boolean success, String errorMsg,
                              Integer inputTokens, Integer outputTokens,
                              int durationMs, Instant createdAt) {
}
