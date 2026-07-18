# LLM 网关需求规格

## 1. 概述

参照 cc-switch 的 provider 管理和代理转发机制，构建一个 LLM 网关服务。流量链路：

```
Claude Code / OpenAI 客户端 → cc-switch → LLM网关(本项目) → AWS Bedrock
                                                        → HTTP 透传供应商（other-providers，如华为 MaaS、Kimi）
```

网关对外兼容三种 API 协议：

- **Anthropic Messages API**：`POST /v1/messages`
- **OpenAI Chat Completions API**：`POST /v1/chat/completions`
- **OpenAI Responses API**：`POST /v1/responses`

下游支持两类上游：**AWS Bedrock**（SDK 异步调用）与 **HTTP 透传供应商**（`other-providers`）。客户端协议与上游格式不一致时网关自动做协议互转（Anthropic ↔ OpenAI）。

---

## 2. 技术栈

| 项 | 选型 |
|----|------|
| 语言/框架 | Java 21 + Spring Boot 3.x + WebFlux (Reactor) |
| 部署 | K8s，HTTP 仅对内网暴露 |
| 配置中心 | Apollo Java SDK（官方，单 namespace `application`） |
| 数据存储 | MySQL（请求日志、按小时聚合的 token 用量等；请求日志保留约 30 天）+ Flyway 迁移 |
| 缓存 | Caffeine（认证信息、限流窗口） |
| AWS SDK | `software.amazon.awssdk:bedrockruntime` (InvokeModel / InvokeModelWithResponseStream) |

---

## 3. 功能需求

### 3.1 API 入口

1. 提供三个代理端点，均有**去 `/v1` 前缀的兼容路由**（适配把 `/v1` 写进 base_url 的客户端，如 opencode CLI）：

   | 方法 | 路径 | 兼容路由 | 协议 |
   |------|------|----------|------|
   | POST | `/v1/messages` | `/messages` | Anthropic Messages |
   | POST | `/v1/chat/completions` | `/chat/completions` | OpenAI Chat Completions |
   | POST | `/v1/responses` | `/responses` | OpenAI Responses |

2. 认证头：优先 `x-api-key`，回退 `Authorization: Bearer <token>`（见 3.2）。
3. 错误响应按**入口协议**渲染：Anthropic 入口返回 Anthropic 错误 JSON，OpenAI 入口返回 OpenAI 错误 JSON（错误码见 3.12）。

### 3.2 上游认证与鉴权（个人 Token）

个人发起请求时携带**认证 Token**（不再在 Token 内嵌个人 ID）。网关用 Token 向外部认证平台换取个人身份与授权信息。

1. **凭证来源（兼容 Anthropic 习惯）**  
   - 优先：`x-api-key` 头中的值为认证 Token。  
   - 否则：`Authorization: Bearer <token>` 中的值为认证 Token。  
   上述二者至少提供其一，否则返回 HTTP 403。

2. **认证平台调用**  
   - 请求方式：`GET {auth.platform_url}?token={认证Token}`（Token 需 URL 编码）。  
   - 平台 URL 存储在配置 `auth.platform_url` 中。

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
   - 认证平台调用失败、超时或返回格式不符合约定时，**一律拒绝请求**（不使用历史缓存降级放行），返回 HTTP 503。

5. **网关校验**  
   - 校验 `token_expire_at` 未过期。  
   - 校验请求体中的 `model` 在 `models` 列表中且对应 `expire_at` 未过期。  
   - 未授权、模型未列出、模型已过期或 Token 已过期 → 返回 HTTP 403。  
   - 授权有效 → 继续处理；后续链路中的「用户」均指认证返回的 `person_id`。

6. **Mock 模式**：配置 `auth.mock_enabled=true` 时跳过认证、所有请求直接放行，仅供本地开发测试，**生产必须为 false**。

### 3.3 授权信息缓存

1. **缓存维度**：以**认证 Token** 为缓存 key；缓存 value 包含 `person_id`、`token_expire_at`、完整 `models` 列表及各模型 `expire_at`。

2. **缓存介质**：Caffeine 内存缓存；默认配置 TTL 仍为 8 小时（`auth.cache_ttl_seconds`），**实际条目 TTL** 取以下三者的最小值：  
   - 默认 TTL（`auth.cache_ttl_seconds`）；  
   - 距离 `token_expire_at` 的剩余时间；  
   - 所有已返回模型授权中，距离最近一条 `expire_at` 的剩余时间（若无有效模型条目则按约定处理）。

3. **命中后的二次校验（每次请求）**  
   - 若缓存存在，且 Token 未过期、且当前请求的 `model` 在缓存列表中且该模型 `expire_at` 未过期 → 直接使用缓存，不调用认证平台。  
   - 若**无缓存**、**Token 已过期**、**缓存中无该模型**或**该模型已过期** → 必须重新调用认证平台刷新；刷新后仍不满足则返回 HTTP 403。

4. **并发**：同一 Token 的并发刷新应合并为单次认证平台请求（inflight 引用计数共享），避免惊群。

5. **失败策略**：认证平台不可用时**不得**使用过期或陈旧缓存放行；无有效认证结果则拒绝请求（与 3.2 一致）。

### 3.4 模型→上游路由

1. 模型名称与上游模型为**一对一映射**，通过 `model-mappings` 列表配置。每条映射结构：

   ```yaml
   model-mappings:
     - model-name: kimi-k3            # 客户端请求中的 model
       provider: kimi                 # aws 或 other-providers 中的 name
       upstream-model-id: k3          # 实际调用的上游模型 ID
       region: us-west-2              # 可选，仅 Bedrock 生效，缺省用 bedrock.region
       capabilities: [text, vision, tools, stream]
   ```

2. `provider` 决定上游类型：
   - `aws` → AWS Bedrock（见 3.5）；
   - 其他值 → `other-providers` 列表中同名供应商（见 3.6）。

3. 路由时按请求体中的 `model` 精确匹配 `model-name`；**未配置映射的模型直接拒绝**（Anthropic 入口返回 502，OpenAI 入口返回 404 `model_not_found`）。

4. `capabilities` 声明模型能力，影响以下行为：
   - `vision`：是否原样转发图片块（见 3.7）；
   - `stream`：请求体未声明 `stream` 时的流式回退依据。

### 3.5 Bedrock 代理转发

1. **协议**：通过 AWS Bedrock Runtime 异步 SDK 调用：
   - 流式：`InvokeModelWithResponseStream`；
   - 非流式：`InvokeModel`。
2. **请求体**：直接透传，不做格式转换；仅将 `model` 替换为映射的 `upstream-model-id`。
3. **响应格式**：`bedrock.response_format` 控制——`anthropic`（默认，转为 Anthropic 标准格式）或 `passthrough`（透传原始响应）。
4. **超时与重试**：`upstream.timeout_seconds`（默认 300 秒，覆盖流式整个生命周期）、`upstream.retry_max`（默认 3 次），Bedrock 与 HTTP 透传共用。
5. **AWS 凭证**：优先 `bedrock.access_key` / `bedrock.secret_key`（STS 临时凭证另需 `bedrock.session_token`）走 StaticCredentialsProvider；**未配置时走 IRSA / 默认凭证链**。
6. **Region**：默认 `bedrock.region`（`us-east-1`），模型映射可单独覆盖。

### 3.6 HTTP 透传供应商（other-providers）

1. 与 Bedrock 平行的通用 HTTP 上游，通过 `other-providers` 列表配置：

   ```yaml
   other-providers:
     - name: huawei
       api-format: anthropic        # 可选：anthropic / openai / 不配置
       base-url: https://example.com/anthropic
       api-keys:                    # 可配 1 个或多个
         - ${HUAWEI_MAAS_API_KEY:}
   ```

2. **`api-format` 语义**：
   - `anthropic`：`base-url` 指向 Anthropic 根路径，请求 `POST {base-url}/v1/messages`，鉴权头 `x-api-key`；
   - `openai`：`base-url` 指向 OpenAI 根路径，请求 `POST {base-url}/chat/completions`，鉴权头 `Authorization: Bearer`；
   - **不配置**：按客户端协议完全透传（Anthropic↔Anthropic、OpenAI↔OpenAI），不做格式互转。

3. **多 Key 负载与故障转移**：
   - 一个供应商可配置多个 `api-keys`，每次请求**均匀随机**选取，降低单 Key 限流风险；
   - 上游返回鉴权/配额类错误（**401/403/429**）且**尚未向下游发出任何事件**时，排除已失败的 key 换下一个重试；
   - 已开始输出事件的流不能安全重试，直接透传错误。

4. **健壮性约束**：
   - 上游以 HTTP 200 返回 JSON 错误体（部分供应商的错误包装方式）时，必须识别为错误而非空流；
   - 上游 Anthropic SSE 缺 `event:` 行时，从 `data` JSON 的 `type` 字段补齐事件名，保证 Claude Code 能解析；
   - 上游返回空 SSE 流时视为供应商错误（502）。

### 3.7 协议互转与视觉处理

1. **协议互转**：客户端协议与上游 `api-format` 不一致时自动转换：
   - Anthropic 客户端 → OpenAI 上游：请求转 OpenAI Chat Completions，响应 SSE 逐 chunk 转回 Anthropic 事件序列；
   - OpenAI 客户端 → Anthropic 上游：请求转 Anthropic Messages，响应转 OpenAI chunk；
   - OpenAI Responses 入口的请求同样支持转换到上游格式。
2. **工具调用保真**：`tool_calls` / `tool_call_id` 全链路保留，转换不得丢失工具调用上下文。
3. **视觉模型感知**：
   - 映射 `capabilities` 含 `vision` 的模型：图片块**原样转发**；
   - 非视觉模型：自动将 `image` / `image_url` 块**替换为占位文本**（避免长会话历史中残留图片导致上游 400）；剥离失败返回 400；
   - OpenAI 风格的 `image_url` 块（data URI / http(s) URL）在 Anthropic 入口统一**归一化为 Anthropic `image` 块**。

### 3.8 流式与非流式

1. **流式判定**：以请求体 `stream` 字段为准；未声明时按 Anthropic 契约视为非流式（HTTP 透传链路可回退到 `capabilities` 中的 `stream`）。
2. **SSE 流式转发**：上游逐 chunk 透传，不等待完整响应；每 15 秒发送 SSE 注释帧（`: ping`）心跳保活。
3. **非流式（`stream:false`）**：上游按非流式调用，网关聚合结果为**完整 message JSON** 一次性返回（Anthropic 入口聚合 SSE 事件序列还原为完整 message）。
4. **客户端中途取消**：流式场景下客户端断开时，若已产生上游 token 消耗，仍写入请求日志（见 3.10）。

### 3.9 配置管理（Apollo）

1. 所有配置存储在一个 Apollo namespace `application` 中（本地开发可落在 `application.yml` / 环境变量）。
2. 配置项清单：

   | Key | 默认值 | 说明 |
   |-----|--------|------|
   | `model-mappings` | `[]` | 模型映射列表：model-name / provider / upstream-model-id / region / capabilities |
   | `other-providers` | `[]` | HTTP 透传供应商：name / base-url / api-format / api-keys |
   | `upstream.retry_max` | 3 | 上游调用最大重试次数（Bedrock 与透传共用） |
   | `upstream.timeout_seconds` | 300 | 上游请求超时秒数，覆盖流式整个生命周期 |
   | `bedrock.region` | `us-east-1` | AWS 默认 region（映射可单独覆盖） |
   | `bedrock.access_key` / `bedrock.secret_key` / `bedrock.session_token` | 空 | AWS 凭证；空时走 IRSA / 默认凭证链 |
   | `bedrock.response_format` | `anthropic` | Bedrock 响应转 Anthropic 格式 / `passthrough` 透传 |
   | `bedrock.tool_call_trace_enabled` | false | 工具调用追踪日志（仅调试用） |
   | `auth.platform_url` | （必填） | 外部认证平台地址 |
   | `auth.platform_timeout_seconds` | 5 | 外部平台请求超时秒数 |
   | `auth.cache_ttl_seconds` | 28800 | 授权缓存 TTL（8 小时） |
   | `auth.mock_enabled` | false | Mock 模式开关（生产必须 false） |
   | `rate_limit.default_rpm` | 60 | 每 `person_id` 每分钟最大请求数 |
   | `spring.codec.max-in-memory-size` | 32MB | 请求/响应体聚合内存上限（base64 图片场景需要） |

3. 网关启动时从 Apollo 拉取全量配置，**Apollo 不可用时启动失败**（不降级）。
4. 运行时通过 Apollo 长轮询接收变更通知，各实例内存热加载，无需重启（限流规则等支持热更新）。

### 3.10 请求日志与用量记录

#### 3.10.1 单次请求日志（`request_logs`）

1. 每次完成或失败的上游相关请求，记录以下字段写入 MySQL（表字段名 `username` 沿用历史命名，**语义为认证返回的 `person_id`**）：

   | 字段 | 类型 | 说明 |
   |------|------|------|
   | username | VARCHAR(128) | 认证平台返回的 `person_id` |
   | model | VARCHAR(128) | 请求中的 model 字段 |
   | provider | VARCHAR(16) | 上游渠道：`aws` 或 other-providers 中的 name |
   | provider_id | VARCHAR(64) | 实际路由到的上游模型 ID |
   | success | TINYINT | 1=成功, 0=失败 |
   | error_msg | TEXT | 失败时的错误信息（客户端中途取消记为 `client_cancelled`） |
   | input_tokens | INT | **上游**响应 usage 中的输入 token；无消耗可为空 |
   | output_tokens | INT | **上游**响应 usage 中的输出 token；无消耗可为空 |
   | duration_ms | INT | 请求耗时（毫秒） |
   | created_at | DATETIME | 记录写入时间 |

2. **计入规则**：仅统计**真实消耗上游 token** 的请求；未到达上游、无 usage 的请求不计入 token 字段。流式场景下客户端中途取消：若已产生上游 input/output token，仍应写入本次日志（含 token 字段），以便与 3.10.2 用量一致。

3. **异步批量写入**（fire-and-forget，不阻塞 Reactor 事件循环）：

   | 参数 | 值 |
   |------|-----|
   | 内存缓冲 | 1000 条 |
   | 刷新间隔 | 5 秒 |
   | 批量 INSERT | 500 条/批 |

4. **保留策略**：请求日志数据保留约 30 天（与概述一致）；自动清理策略可后续补充。

#### 3.10.2 个人 Token 用量（按小时聚合）

1. **维度**：以 `person_id` 为统计主体，**不按 Token** 分列（同人单 Token 前提下与业务一致）。

2. **时间窗口**：按**北京时间**（`Asia/Shanghai`）自然小时对齐；对 `input_tokens`、`output_tokens` **分别累加**。

3. **数据来源**：与单次请求日志一致，仅在有上游 token 消耗时累加。

4. **存储**：两张聚合表，均按小时窗口 upsert 累加：
   - `person_token_usage_hourly`：按 `(person_id, provider, window_start)` 唯一约束——个人 × 上游渠道维度；
   - `person_model_token_usage_hourly`：按 `(person_id, model, provider, window_start)` 唯一约束——个人 × 模型 × 上游渠道维度。

   `window_start` 为该小时起始时间（北京时间语义下的整点）。

#### 3.10.3 请求内容留存（占位）

1. **目标**：后续支持「记录每次请求的详细内容」，并约定约 **1 个月** 删除周期（实现时可配合定时任务或运维策略）。

2. **当前阶段**：产品需求明确**先不落库、不采集全文**；网关在代码中保留扩展点（占位方法/接口），待确定脱敏、截断、加密与字段设计后再实现。

3. **安全约束（实现时须遵守）**：请求体可能含 prompt、代码、密钥等敏感信息，正式实现前须完成脱敏方案与最大长度限制评审。

### 3.11 速率限制

1. 按 **`person_id`**（认证平台返回的个人标识）进行速率限制，而非按原始 Token 字符串。
2. **算法**：加权滑动窗口计数——预估值 = 当前分钟窗口计数 + 上一分钟窗口计数 × (1 − 当前窗口已过去比例)，抑制固定窗口边界处约 2×RPM 的突发流量。每用户仅保存两个计数器（O(1) 内存），窗口 5 分钟无访问自动清理。
3. 默认限制：每分钟每 `person_id` `rate_limit.default_rpm` 次，通过 Apollo 热更新。
4. 超出限制返回 HTTP 429，响应体格式兼容入口协议的错误格式。
5. **单实例约束**：限流为单实例内存实现，K8s 多副本部署时实际限额为 **副本数 × RPM**。

### 3.12 错误响应

错误响应兼容各入口协议格式（Anthropic 入口为 Anthropic 错误 JSON，OpenAI 入口为 OpenAI 错误 JSON）：

| HTTP 状态码 | error.type | 触发场景 |
|-------------|------------|----------|
| 400 | `invalid_request_error` | 请求体缺 model / 图片块处理失败 |
| 403 | `permission_error` | 缺认证头 / 认证失败 / 模型未授权 |
| 404 | `model_not_found` | 模型未配置映射（OpenAI 入口） |
| 429 | `rate_limit_error` | 触发限流 |
| 502 | `api_error` | 上游供应商调用失败 / 模型未配置映射（Anthropic 入口） |
| 503 | `api_error` | 认证平台不可用 |
| 504 | `timeout_error` | 上游调用超时 |

### 3.13 健康检查

- `GET /health` → 200，供 K8s liveness/readiness probe 使用。

---

## 4. 非功能需求

- **高可用**：K8s 多实例部署，无状态设计，配置通过 Apollo 同步。
- **优雅关闭**：收到 SIGTERM 后停止接受新请求，等待进行中请求完成后退出（等待上限默认 30 秒，`spring.lifecycle.timeout-per-shutdown-phase` 可配）。
- **可观测**：结构化日志（JSON 格式），输出到 stdout，K8s 日志采集；请求链路带 traceId 贯穿上游调用日志。
- **非阻塞**：全链路 WebFlux 非阻塞 I/O；日志写入 fire-and-forget，不阻塞请求。
