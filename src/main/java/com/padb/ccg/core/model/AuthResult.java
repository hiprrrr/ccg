package com.padb.ccg.core.model;

import java.time.Instant;
import java.util.List;

/**
 * 认证结果，表示认证平台根据请求 Token 返回的个人身份和模型授权信息
 *
 * @param personId      个人 ID，用于限流、日志和 token 用量统计
 * @param tokenExpireAt Token 过期时间
 * @param models        授权模型列表及各模型过期时间
 */
public record AuthResult(String personId, Instant tokenExpireAt, List<ModelAuthorization> models) {
}
