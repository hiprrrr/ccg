# ccgateway

LLM API 网关，部署在 K8s 上。对外兼容 Anthropic Messages、OpenAI Chat Completions、OpenAI Responses 三种 API 格式；下游支持 AWS Bedrock 和通用 HTTP 透传供应商（华为 MaaS、Kimi 等），并按需做协议互转。

```
Claude Code / OpenAI 客户端 → cc-switch → ccgeteway(本项目) → AWS Bedrock
                                                            → HTTP 透传供应商（other-providers）
```

## 核心能力

- **三协议入口**：`/v1/messages`（Anthropic）、`/v1/chat/completions`、`/v1/responses`（OpenAI），均有去 `/v1` 前缀的兼容路由
- **双类上游**：AWS Bedrock（SDK 异步调用）；other-providers（HTTP 透传，支持 `api-format: anthropic / openai / 不配置按客户端协议透传`）
- **协议互转**：客户端协议与上游格式不一致时自动转换（Anthropic ↔ OpenAI），tool_calls / tool_call_id 全链路保留
- **多 Key 负载与故障转移**：一个供应商可配多个 api-key，均匀随机选取；遇 401/403/429 且响应尚未发出时自动换 key 重试
- **流式与非流式**：SSE 流式转发（带心跳保活）；`stream:false` 聚合上游结果为完整 JSON 返回
- **视觉模型感知**：非视觉模型自动将图片块替换为占位文本，视觉模型原样转发
- **认证 + 限流**：外部认证平台（结果缓存 8h、并发请求共享 inflight）；加权滑动窗口限流（单实例）

## 技术栈

- **Java 21** + **Spring Boot 3.3** + **WebFlux (Reactor)**
- **AWS Bedrock Runtime** — InvokeModel / InvokeModelWithResponseStream 异步调用
- **Caffeine** — 本地缓存（认证信息 / 限流窗口）
- **MySQL + Flyway** — 请求日志持久化（异步批量写入）
- **Apollo** — 配置中心
- **Maven** — 构建管理（单模块）

## 快速开始

### 环境要求

- JDK 21+
- Maven 3.8+
- MySQL 8.0+
- 至少一类上游：AWS Bedrock 凭证，或 HTTP 透传供应商的 API Key

### 本地运行

1. 创建数据库（Flyway 会自动建表）：

```sql
CREATE DATABASE ccg;
```

2. 配置 `application.yml` 或通过环境变量 / Apollo 配置必填项：

```yaml
# 模型映射：provider 为 aws 或 other-providers 中的 name
model-mappings:
  - model-name: claude-opus-4-7
    provider: aws
    upstream-model-id: us.anthropic.claude-opus-4-7-v1:0
    region: us-west-2
    capabilities: [text, vision, tools, stream]

# HTTP 透传供应商（可选，与 Bedrock 平行）
other-providers:
  - name: huawei
    api-format: anthropic        # anthropic / openai / 不配置按客户端协议透传
    base-url: https://example.com/anthropic
    api-keys:                    # 可配多个，随机负载 + 故障转移
      - ${HUAWEI_MAAS_API_KEY:}

bedrock:
  access-key: ${AWS_ACCESS_KEY_ID:}      # 未配置时走 IRSA / 默认凭证链
  secret-key: ${AWS_SECRET_ACCESS_KEY:}

auth:
  platform-url: http://localhost:8081/api/auth
  mock-enabled: true             # 本地开发可开启 mock 模式跳过认证，生产必须 false

rate-limit:
  default-rpm: 60
```

3. 启动应用：

```bash
mvn spring-boot:run
```

4. 验证：

```bash
# 健康检查
curl http://localhost:8080/health

# Anthropic 格式（stream:false 返回完整 JSON）
curl -X POST http://localhost:8080/v1/messages \
  -H "Content-Type: application/json" \
  -H "x-api-key: your-username" \
  -d '{"model":"claude-opus-4-7","stream":false,"max_tokens":1024,"messages":[{"role":"user","content":"Hello"}]}'

# OpenAI 格式
curl -X POST http://localhost:8080/v1/chat/completions \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer your-username" \
  -d '{"model":"claude-opus-4-7","stream":true,"messages":[{"role":"user","content":"Hello"}]}'
```

## 项目结构

```
src/main/java/com/padb/ccg/
├── server/          # 应用入口、路由、全局错误处理、优雅关闭
├── auth/            # 认证服务、缓存管理（inflight 引用计数共享）、认证平台客户端
├── proxy/           # 代理编排（Anthropic/OpenAI 服务）、协议转换器、上游路由、
│                    #   Bedrock 与 HTTP 透传 handler、限流器、SSE 转换/聚合
├── routing/         # 模型→供应商路由映射
├── logging/         # 异步批量日志写入
└── core/
    ├── spi/         # SPI 接口（模块间契约）
    ├── model/       # 领域模型（records）
    └── exception/   # 业务异常体系
```

## API 端点

| 方法 | 路径 | 说明 |
|------|------|------|
| POST | `/v1/messages`（`/messages`） | Anthropic Messages API 兼容端点 |
| POST | `/v1/chat/completions`（`/chat/completions`） | OpenAI Chat Completions 兼容端点 |
| POST | `/v1/responses`（`/responses`） | OpenAI Responses API 兼容端点 |
| GET | `/health` | 健康检查（K8s probe） |

认证头：优先 `x-api-key`，回退 `Authorization: Bearer <token>`。

## 配置项

| Key | 默认值 | 说明 |
|-----|--------|------|
| `model-mappings` | `[]` | 模型映射列表：model-name / provider / upstream-model-id / region / capabilities |
| `other-providers` | `[]` | HTTP 透传供应商：name / base-url / api-format / api-keys（多 key 负载） |
| `upstream.retry_max` | 3 | 上游调用最大重试次数 |
| `upstream.timeout_seconds` | 300 | 上游请求超时（秒），覆盖流式整个生命周期 |
| `bedrock.region` | `us-east-1` | AWS 默认区域（映射可单独覆盖） |
| `bedrock.access_key` / `secret_key` / `session_token` | 空 | AWS 凭证；空时走 IRSA / 默认凭证链 |
| `bedrock.response_format` | `anthropic` | Bedrock 响应转 Anthropic 格式 / `passthrough` 透传 |
| `bedrock.tool_call_trace_enabled` | false | 工具调用追踪日志（仅调试用） |
| `auth.platform_url` | （必填） | 认证平台地址 |
| `auth.platform_timeout_seconds` | 5 | 认证平台超时（秒） |
| `auth.cache_ttl_seconds` | 28800 | 认证缓存 TTL（秒） |
| `auth.mock_enabled` | false | Mock 模式开关（生产必须 false） |
| `rate_limit.default_rpm` | 60 | 每用户每分钟请求上限（加权滑动窗口，单实例限额；多副本实际为 N×RPM） |
| `spring.codec.max-in-memory-size` | 32MB | 请求/响应体聚合内存上限（base64 图片场景需要） |

## 错误响应

错误响应兼容各入口协议格式（Anthropic 入口为 Anthropic 错误 JSON，OpenAI 入口为 OpenAI 错误 JSON）：

```json
{
  "type": "error",
  "error": {
    "type": "permission_error",
    "message": "Model 'xxx' not authorized for user 'xxx'"
  }
}
```

| HTTP 状态码 | error.type | 触发场景 |
|-------------|------------|----------|
| 400 | `invalid_request_error` | 请求体缺 model / 非视觉模型携带图片 |
| 403 | `permission_error` | 缺认证头 / 认证失败 / 模型未授权 |
| 404 | `model_not_found` | 模型未配置映射（OpenAI 入口） |
| 429 | `rate_limit_error` | 触发限流 |
| 502 | `api_error` | 上游供应商调用失败 |
| 503 | `api_error` | 认证平台不可用 |
| 504 | `timeout_error` | 上游调用超时 |

## 部署

```bash
kubectl apply -f deploy/k8s-config.yml
kubectl apply -f deploy/k8s-deployment.yml
kubectl apply -f deploy/k8s-service.yml
```
