# CLAUDE.md - AI 编程助手指南

## 核心原则

### 1. 先思考后编码
- **不要急于写代码**：在动手前，先仔细分析问题，明确目标。
- **提出澄清性问题**：如果需求模糊，主动询问细节，而不是自行假设。
- **规划步骤**：在脑海中或输出中列出实现方案的关键步骤。

### 2. 简洁至上
- **避免过度设计**：用最简单、最直接的方式解决问题，不要提前引入复杂的抽象或模式。
- **拒绝"炫技"**：不要为了展示能力而使用复杂语法、新框架或多余的优化。
- **代码即文档**：变量、函数命名要清晰，逻辑要直白。

### 3. 精准修改
- **最小化改动**：只修改必要的代码行，不要顺便"重构"无关部分。
- **解释改动原因**：每次修改后，用一句话说明为什么这么改。
- **避免连锁反应**：改动一个函数时，检查是否影响到调用它的地方，并同步更新。

### 4. 目标驱动执行
- **明确最终目标**：在开始任何任务前，确认用户真正想要的结果是什么。
- **分解子任务**：将大任务拆解为可独立验证的小步骤，每完成一步就确认效果。
- **及时反馈**：如果遇到阻塞或需要决策，主动向用户提问，而不是自己"猜"。

---

## 项目上下文

**ccgeteway** — LLM API 网关，部署在 K8s 上。上游是 Claude Code（通过 cc-switch 代理），下游是 Anthropic API 格式的模型供应商。

**代码目录**: `/Users/hipr/Work/project/ccgeteway`

**技术约束**:
- Java 21, Spring Boot 3.x + WebFlux (Reactor), Maven 单模块
- 配置中心: Apollo（单 namespace `application`）
- 缓存: Caffeine
- 数据库: MySQL（仅用于请求日志）
- 迁移工具: Flyway
- 日志输出: JSON 格式 → stdout

**需求文档**: `docs/requirement.md`
**架构计划**: 参照 `.claude/plans/` 目录下的最新计划文件

---

## 编码规范

1. **Java records** — 领域模型（DTO、配置）优先使用 record，除非需要继承
2. **构造函数注入** — 不使用 `@Autowired` 字段注入
3. **WebFlux 非阻塞** — 所有 I/O 操作必须返回 Mono/Flux，不要 `.block()`
4. **异常统一处理** — 抛 GatewayException 子类，由 GlobalErrorHandler 统一转换为 Anthropic 兼容错误 JSON
5. **SPI 接口优先** — 模块间通过 gateway-core 中的接口通信，不直接依赖实现类
6. **日志不阻塞请求** — 日志写入用 fire-and-forget，不要阻塞 Reactor 事件循环
7. **线程安全** — 热更新的配置状态用 AtomicReference 或 AtomicInteger 保护
8. **代码注释** - 所有的变量、方法及复杂的代码都需要有详细的中文注释

<!-- gitnexus:start -->
# GitNexus — Code Intelligence

This project is indexed by GitNexus as **ccg** (1529 symbols, 3571 relationships, 132 execution flows). Use the GitNexus MCP tools to understand code, assess impact, and navigate safely.

> If any GitNexus tool warns the index is stale, run `npx gitnexus analyze` in terminal first.

## Always Do

- **MUST run impact analysis before editing any symbol.** Before modifying a function, class, or method, run `gitnexus_impact({target: "symbolName", direction: "upstream"})` and report the blast radius (direct callers, affected processes, risk level) to the user.
- **MUST run `gitnexus_detect_changes()` before committing** to verify your changes only affect expected symbols and execution flows.
- **MUST warn the user** if impact analysis returns HIGH or CRITICAL risk before proceeding with edits.
- When exploring unfamiliar code, use `gitnexus_query({query: "concept"})` to find execution flows instead of grepping. It returns process-grouped results ranked by relevance.
- When you need full context on a specific symbol — callers, callees, which execution flows it participates in — use `gitnexus_context({name: "symbolName"})`.

## Never Do

- NEVER edit a function, class, or method without first running `gitnexus_impact` on it.
- NEVER ignore HIGH or CRITICAL risk warnings from impact analysis.
- NEVER rename symbols with find-and-replace — use `gitnexus_rename` which understands the call graph.
- NEVER commit changes without running `gitnexus_detect_changes()` to check affected scope.

## Resources

| Resource | Use for |
|----------|---------|
| `gitnexus://repo/ccg/context` | Codebase overview, check index freshness |
| `gitnexus://repo/ccg/clusters` | All functional areas |
| `gitnexus://repo/ccg/processes` | All execution flows |
| `gitnexus://repo/ccg/process/{name}` | Step-by-step execution trace |

## CLI

| Task | Read this skill file |
|------|---------------------|
| Understand architecture / "How does X work?" | `.claude/skills/gitnexus/gitnexus-exploring/SKILL.md` |
| Blast radius / "What breaks if I change X?" | `.claude/skills/gitnexus/gitnexus-impact-analysis/SKILL.md` |
| Trace bugs / "Why is X failing?" | `.claude/skills/gitnexus/gitnexus-debugging/SKILL.md` |
| Rename / extract / split / refactor | `.claude/skills/gitnexus/gitnexus-refactoring/SKILL.md` |
| Tools, resources, schema reference | `.claude/skills/gitnexus/gitnexus-guide/SKILL.md` |
| Index, status, clean, wiki CLI commands | `.claude/skills/gitnexus/gitnexus-cli/SKILL.md` |

<!-- gitnexus:end -->
