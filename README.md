# ccgateway

LLM API 网关，部署在 K8s 上，对外兼容 Anthropic Messages API，将请求代理转发到 AWS Bedrock。

```
Claude Code → cc-switch → ccgeteway(本项目) → AWS Bedrock
```

## 技术栈

- **Java 21** + **Spring Boot 3.3** + **WebFlux (Reactor)**
- **AWS Bedrock Runtime** — InvokeModelWithResponseStream 流式调用
- **Caffeine** — 本地缓存（认证信息 / 限流窗口）
- **MySQL** — 请求日志持久化
- **Apollo** — 配置中心，热更新
- **Maven** — 构建管理

## 快速开始

### 环境要求

- JDK 21+
- Maven 3.8+
- MySQL 8.0+
- 可访问的 AWS Bedrock 端点

### 本地运行

1. 创建数据库：

```sql
CREATE DATABASE ccg;
```

2. 配置 `application.yml` 或通过 Apollo 配置以下必填项：

```yaml
bedrock:
  access_key: <your-aws-access-key>
  secret_key: <your-aws-secret-key>
  model-mappings:
    - id: mapping-001
      modelName: claude-opus-4-7
      bedrockModelId: us.anthropic.claude-opus-4-7-v1:0
      region: us-west-2
      capabilities: [text, vision]

auth:
  platform_url: <auth-platform-url>
  mock-enabled: true  # 本地开发可开启 mock 模式跳过认证

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

# 发送消息
curl -X POST http://localhost:8080/v1/messages \
  -H "Content-Type: application/json" \
  -H "x-api-key: your-username" \
  -d '{"model":"claude-opus-4-7","messages":[{"role":"user","content":"Hello"}]}'
```

## 项目结构

```
src/main/java/com/padb/ccg/
├── server/          # 应用入口、路由、全局错误处理、优雅关闭
├── auth/            # 认证服务、缓存管理、认证平台客户端
├── proxy/           # Bedrock 代理转发、限流器、SSE 流式响应
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
| POST | `/v1/messages` | Anthropic Messages API 兼容端点 |
| GET | `/health` | 健康检查（K8s probe） |

## 配置项

| Key | 默认值 | 说明 |
|-----|--------|------|
| `bedrock.model_mappings` | `[]` | 模型→Bedrock 映射 JSON 数组 |
| `bedrock.region` | `us-east-1` | AWS 默认区域 |
| `bedrock.access_key` | （必填） | AWS Access Key |
| `bedrock.secret_key` | （必填） | AWS Secret Key |
| `bedrock.retry_max` | 3 | 最大重试次数 |
| `bedrock.timeout_seconds` | 120 | 调用超时（秒） |
| `auth.platform_url` | （必填） | 认证平台地址 |
| `auth.platform_timeout_seconds` | 5 | 认证平台超时（秒） |
| `auth.cache_ttl_seconds` | 28800 | 认证缓存 TTL（秒） |
| `auth.mock_enabled` | false | Mock 模式开关 |
| `rate_limit.default_rpm` | 60 | 每用户每分钟请求上限 |

## 错误响应

错误响应兼容 Anthropic API 格式：

```json
{
  "type": "error",
  "error": {
    "type": "permission_error",
    "message": "Model 'xxx' not authorized for user 'xxx'"
  }
}
```

| HTTP 状态码 | error.type |
|-------------|------------|
| 403 | `permission_error` |
| 429 | `rate_limit_error` |
| 502 | `api_error` |
| 504 | `timeout_error` |

## 部署

```bash
kubectl apply -f deploy/k8s-config.yml
kubectl apply -f deploy/k8s-deployment.yml
kubectl apply -f deploy/k8s-service.yml
```
