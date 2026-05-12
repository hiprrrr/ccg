# LLM 网关需求规格

## 1. 概述

参照 cc-switch 的 provider 管理和代理转发机制，构建一个 LLM 网关服务。流量链路：

```
Claude Code → cc-switch → LLM网关(本项目) → AWS Bedrock
```

网关暴露 `/v1/messages` 端点，对外完全兼容 Anthropic Messages API。所有模型通过 AWS Bedrock Runtime 的 InvokeModel API 调用下游模型，请求体和响应体直接透传，不做格式转换。

---

## 2. 技术栈

| 项 | 选型 |
|----|------|
| 语言/框架 | Java 21 + Spring Boot 3.x + WebFlux (Reactor) |
| 部署 | K8s，HTTP 仅对内网暴露 |
| 配置中心 | Apollo Java SDK（官方，单 namespace `application`） |
| 数据存储 | MySQL（请求日志、用量记录，保留 30 天） |
| AWS SDK | `software.amazon.awssdk:bedrockruntime` (InvokeModelWithResponseStream) |

---

## 3. 功能需求

### 3.1 上游认证与鉴权

1. 上游请求在 HTTP Header `x-api-key` 中传入用户名（兼容 Anthropic API）。
2. 网关根据用户名，向外部授权平台发起 GET 请求，获取该用户已授权的模型列表。
3. 外部授权平台接口：
   - 请求方式：`GET {auth.platform_url}?username={用户名}`
   - 平台 URL 存储在 Apollo 配置中
   - 响应结构：
     ```json
     {
       "models": [
         {"name": "claude-opus-4-7", "expire_at": "2026-06-12T00:00:00Z"},
         {"name": "claude-sonnet-4-6", "expire_at": "2026-06-12T00:00:00Z"}
       ]
     }
     ```
4. 网关校验请求中 `model` 字段是否在授权列表内且未过期：
   - 未授权/已过期 → 返回 HTTP 403，拒绝请求
   - 授权有效 → 继续处理

### 3.2 授权信息缓存

1. 授权信息通过 Caffeine Cache 在内存中缓存。
2. 默认 TTL：8 小时（可通过 Apollo `auth.cache_ttl_seconds` 调整）。
3. 如果某条授权记录的到期时间早于默认 TTL，则以到期时间作为实际 TTL。
4. 请求到达时先查缓存：命中且未过期直接使用；未命中或已过期则请求外部平台。
5. 并发请求触发同一 key 刷新时，利用 `ConcurrentHashMap.computeIfAbsent` + `CompletableFuture` 实现 singleflight，只发一次外部请求。
6. 外部平台不可用时，使用已过期的缓存数据作为降级（如果存在）；无缓存则拒绝请求。

### 3.3 模型→Bedrock 路由

1. 模型名称与 Bedrock 模型 ID 为**一对一映射**，通过 Apollo 配置定义。
2. 每条映射配置结构：

   ```json
   {
     "id": "mapping-001",
     "modelName": "claude-opus-4-7",
     "bedrockModelId": "us.anthropic.claude-opus-4-7-v1:0",
     "region": "us-west-2",
     "capabilities": ["text", "vision"]
   }
   ```

   所有映射组成一个 JSON 数组，存储在 Apollo key `bedrock.model_mappings` 中。

3. 路由时根据请求中的 `model` 精确匹配 `modelName`，找到对应的 Bedrock 模型 ID 和 region。

### 3.4 Bedrock 代理转发

1. **协议**：通过 AWS Bedrock Runtime 的 `InvokeModelWithResponseStream` API 调用下游模型。
2. **请求体**：直接透传，不做格式转换。请求体 JSON 直接作为 `InvokeModelWithResponseStream` 的 body 传入。
3. **响应体**：流式响应逐 chunk 透传（Server-Sent Events），网关收到一个 chunk 即转发一个 chunk，不等待完整响应。
4. **超时**：120 秒（可通过 Apollo `bedrock.timeout_seconds` 调整）。
5. **重试**：最多重试 3 次（可通过 Apollo `bedrock.retry_max` 调整）。
6. **AWS 凭证**：从 Apollo `bedrock.access_key` / `bedrock.secret_key` 读取，使用 StaticCredentialsProvider。
7. **请求路径**：`POST /v1/messages`，兼容 Anthropic Messages API。

### 3.5 配置管理（Apollo）

1. 所有配置存储在一个 Apollo namespace `application` 中。
2. 配置项清单：

   | Key | 默认值 | 说明 |
   |-----|--------|------|
   | `bedrock.model_mappings` | `[]` | 模型→Bedrock 映射 JSON 数组 |
   | `bedrock.region` | `us-east-1` | AWS 默认 region |
   | `bedrock.access_key` | （必填） | AWS Access Key |
   | `bedrock.secret_key` | （必填） | AWS Secret Key |
   | `bedrock.retry_max` | 3 | 最大重试次数 |
   | `bedrock.timeout_seconds` | 120 | 请求超时秒数 |
   | `auth.platform_url` | （必填） | 外部授权平台地址 |
   | `auth.platform_timeout_seconds` | 5 | 外部平台请求超时 |
   | `auth.cache_ttl_seconds` | 28800 | 授权缓存 TTL（8 小时） |
   | `rate_limit.default_rpm` | （必填） | 每用户每分钟最大请求数 |

3. 网关启动时从 Apollo 拉取全量配置，**Apollo 不可用时启动失败**（不降级）。
4. 运行时通过 Apollo 长轮询接收变更通知，各实例内存热加载，无需重启。

### 3.6 请求日志与用量记录

1. 每次请求记录以下字段，写入 MySQL：

   | 字段 | 类型 | 说明 |
   |------|------|------|
   | username | VARCHAR(128) | 上游调用方 |
   | model | VARCHAR(128) | 请求中的 model 字段 |
   | provider_id | VARCHAR(64) | 实际路由到的 Bedrock 模型 ID |
   | success | TINYINT | 1=成功, 0=失败 |
   | error_msg | TEXT | 失败时的错误信息 |
   | input_tokens | INT | 从 Bedrock 响应 usage 中提取 |
   | output_tokens | INT | 从 Bedrock 响应 usage 中提取 |
   | duration_ms | INT | 请求耗时（毫秒） |
   | created_at | DATETIME | 请求时间 |

2. **异步批量写入**：

   | 参数 | 值 |
   |------|-----|
   | 内存缓冲 | 1000 条 |
   | 刷新间隔 | 5 秒 |
   | 批量 INSERT | 500 条/批 |

3. 数据保留 30 天，暂时不做自动清理。

### 3.7 速率限制

1. 按用户名（`x-api-key` 的值）进行速率限制。
2. 默认限制：每分钟每用户 `rate_limit.default_rpm` 次，通过 Apollo 配置。
3. 超出限制返回 HTTP 429，响应体格式兼容 Anthropic API 错误格式。
4. 限流规则可通过 Apollo 热更新。

### 3.8 健康检查

- `GET /health` → 200，供 K8s liveness/readiness probe 使用。

---

## 4. 非功能需求

- **高可用**：K8s 多实例部署，无状态设计，配置通过 Apollo 同步。
- **优雅关闭**：收到 SIGTERM 后停止接受新请求，等待进行中请求完成后退出（等待上限可配置，默认 30 秒）。
- **可观测**：结构化日志（JSON 格式），输出到 stdout，K8s 日志采集。
