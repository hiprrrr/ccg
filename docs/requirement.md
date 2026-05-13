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
| 数据存储 | MySQL（请求日志、按小时聚合的 token 用量等；请求日志保留约 30 天） |
| AWS SDK | `software.amazon.awssdk:bedrockruntime` (InvokeModelWithResponseStream) |

---

## 3. 功能需求

### 3.1 上游认证与鉴权（个人 Token）

个人发起请求时携带**认证 Token**（不再在 Token 内嵌个人 ID）。网关用 Token 向外部认证平台换取个人身份与授权信息。

1. **凭证来源（兼容 Anthropic 习惯）**  
   - 优先：`x-api-key` 头中的值为认证 Token。  
   - 否则：`Authorization: Bearer <token>` 中的值为认证 Token。  
   上述二者至少提供其一，否则返回 HTTP 403。

2. **认证平台调用**  
   - 请求方式：`GET {auth.platform_url}?token={认证Token}`（Token 需 URL 编码）。  
   - 平台 URL 存储在 Apollo 配置 `auth.platform_url` 中。

3. **认证平台响应结构**（约定）：
   ```json
   {
     "person_id": "u123",
     "token_expire_at": "2026-06-12T00:00:00Z",
     "models": [
       {"name": "claude-opus-4-7", "expire_at": "2026-06-12T00:00:00Z"},
       {"name": "claude-sonnet-4-6", "expire_at": "2026-06-12T00:00:00Z"}
     ]
   }
   ```
   - `person_id`：个人唯一标识，供限流、日志主体、用量统计使用。  
   - `token_expire_at`：该 Token 的过期时间（ISO-8601）。  
   - `models`：该 Token 当前可访问的模型列表；每项须包含 `name` 与 `expire_at`（模型级授权截止时间）。

4. **业务约束**  
   - 同一自然人同一时刻只存在一个有效 Token；Token 会过期，过期后须重新向认证平台获取新 Token。  
   - 认证平台调用失败、超时或返回格式不符合约定时，**一律拒绝请求**（不使用历史缓存降级放行）。

5. **网关校验**  
   - 校验 `token_expire_at` 未过期。  
   - 校验请求体中的 `model` 在 `models` 列表中且对应 `expire_at` 未过期。  
   - 未授权、模型未列出、模型已过期或 Token 已过期 → 返回 HTTP 403。  
   - 授权有效 → 继续处理；后续链路中的「用户」均指认证返回的 `person_id`。

### 3.2 授权信息缓存

1. **缓存维度**：以**认证 Token** 为缓存 key；缓存 value 包含 `person_id`、`token_expire_at`、完整 `models` 列表及各模型 `expire_at`。

2. **缓存介质**：Caffeine 内存缓存；默认配置 TTL 仍为 8 小时（Apollo `auth.cache_ttl_seconds`），**实际条目 TTL** 取以下三者的最小值：  
   - 默认 TTL（`auth.cache_ttl_seconds`）；  
   - 距离 `token_expire_at` 的剩余时间；  
   - 所有已返回模型授权中，距离最近一条 `expire_at` 的剩余时间（若无有效模型条目则按约定处理）。

3. **命中后的二次校验（每次请求）**  
   - 若缓存存在，且 Token 未过期、且当前请求的 `model` 在缓存列表中且该模型 `expire_at` 未过期 → 直接使用缓存，不调用认证平台。  
   - 若**无缓存**、**Token 已过期**、**缓存中无该模型**或**该模型已过期** → 必须重新调用认证平台刷新；刷新后仍不满足则返回 HTTP 403。

4. **并发**：同一 Token 的并发刷新应合并为单次认证平台请求（singleflight），避免惊群。

5. **失败策略**：认证平台不可用时**不得**使用过期或陈旧缓存放行；无有效认证结果则拒绝请求（与 3.1 一致）。

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
   | `rate_limit.default_rpm` | （必填） | 每 `person_id` 每分钟最大请求数 |

3. 网关启动时从 Apollo 拉取全量配置，**Apollo 不可用时启动失败**（不降级）。
4. 运行时通过 Apollo 长轮询接收变更通知，各实例内存热加载，无需重启。

### 3.6 请求日志与用量记录

#### 3.6.1 单次请求日志（`request_logs`）

1. 每次完成或失败的上游相关请求，记录以下字段写入 MySQL（表字段名 `username` 沿用历史命名，**语义为认证返回的 `person_id`**）：

   | 字段 | 类型 | 说明 |
   |------|------|------|
   | username | VARCHAR(128) | 认证平台返回的 `person_id` |
   | model | VARCHAR(128) | 请求中的 model 字段 |
   | provider_id | VARCHAR(64) | 实际路由到的 Bedrock 模型 ID |
   | success | TINYINT | 1=成功, 0=失败 |
   | error_msg | TEXT | 失败时的错误信息 |
   | input_tokens | INT | **上游 Bedrock** 响应 usage 中的输入 token；无消耗可为空 |
   | output_tokens | INT | **上游 Bedrock** 响应 usage 中的输出 token；无消耗可为空 |
   | duration_ms | INT | 请求耗时（毫秒） |
   | created_at | DATETIME | 记录写入时间 |

2. **计入规则**：仅统计**真实消耗上游（Bedrock）token** 的请求；未到达上游、无 usage 的请求不计入 token 字段。流式场景下客户端中途取消：若已产生上游 input/output token，仍应写入本次日志（含 token 字段），以便与 3.6.2 用量一致。

3. **异步批量写入**：

   | 参数 | 值 |
   |------|-----|
   | 内存缓冲 | 1000 条 |
   | 刷新间隔 | 5 秒 |
   | 批量 INSERT | 500 条/批 |

4. **保留策略**：请求日志数据保留约 30 天（与概述一致）；自动清理策略可后续补充。

#### 3.6.2 个人 Token 用量（按小时聚合）

1. **维度**：以 `person_id` 为统计主体，**不按 Token** 分列（同人单 Token 前提下与业务一致）。

2. **时间窗口**：按**北京时间**（`Asia/Shanghai`）自然小时对齐；同一 `person_id` 在同一小时窗口内对 `input_tokens`、`output_tokens` **分别累加**。

3. **数据来源**：与单次请求日志一致，仅在有上游 token 消耗时累加；与 `request_logs` 写入同批或同一事务策略以实现上可接受为准（需求上要求结果一致）。

4. **存储**：独立表（如 `person_token_usage_hourly`），以 `(person_id, window_start)` 唯一约束支持 upsert 累加；`window_start` 为该小时起始时间（北京时间语义下的整点）。

#### 3.6.3 请求内容留存（占位）

1. **目标**：后续支持「记录每次请求的详细内容」，并约定约 **1 个月** 删除周期（实现时可配合定时任务或运维策略）。

2. **当前阶段**：产品需求明确**先不落库、不采集全文**；网关在代码中保留扩展点（占位方法/接口），待确定脱敏、截断、加密与字段设计后再实现。

3. **安全约束（实现时须遵守）**：请求体可能含 prompt、代码、密钥等敏感信息，正式实现前须完成脱敏方案与最大长度限制评审。

### 3.7 速率限制

1. 按 **`person_id`**（认证平台返回的个人标识）进行速率限制，而非按原始 Token 字符串。
2. 默认限制：每分钟每 `person_id` `rate_limit.default_rpm` 次，通过 Apollo 配置。
3. 超出限制返回 HTTP 429，响应体格式兼容 Anthropic API 错误格式。
4. 限流规则可通过 Apollo 热更新。

### 3.8 健康检查

- `GET /health` → 200，供 K8s liveness/readiness probe 使用。

---

## 4. 非功能需求

- **高可用**：K8s 多实例部署，无状态设计，配置通过 Apollo 同步。
- **优雅关闭**：收到 SIGTERM 后停止接受新请求，等待进行中请求完成后退出（等待上限可配置，默认 30 秒）。
- **可观测**：结构化日志（JSON 格式），输出到 stdout，K8s 日志采集。
