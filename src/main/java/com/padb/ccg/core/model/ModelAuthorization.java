package com.padb.ccg.core.model;

import java.time.Instant;

/**
 * 模型授权记录，表示用户对某个模型的访问权限及其过期时间
 *
 * @param name     模型名称
 * @param expireAt 授权过期时间，{@link Instant#MAX} 表示永不过期
 */
public record ModelAuthorization(String name, Instant expireAt) {
}
