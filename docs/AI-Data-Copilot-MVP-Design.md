# AI Data Copilot MVP 技术方案设计

> Phase 1：最小可用版本，聚焦"能对话、能查数、安全可控"。

## 1. 概述

### 1.1 目标

实现用户通过自然语言发起数据查询，AI 自动生成 SQL 并执行，返回结果。核心验证 LLM + Tool 架构的可行性。

### 1.2 MVP 范围

| 功能 | 说明 | 是否 MVP |
|---|---|---|
| Chat API（流式响应） | 用户发起对话，实时返回进度 | 是 |
| `run_sql` 工具 | 执行只读 SQL 查询 | 是 |
| 意图识别 | 规则匹配 + Embedding 分类 | 是 |
| ReAct 执行循环 | 固定最多 3 轮 Tool 调用 | 是 |
| SQL 安全校验 | 只读账号 + AST 解析 + LIMIT + 白名单 | 是 |
| Redis 短期记忆 | 最近 5 轮对话上下文 | 是 |
| 基础监控 | Prometheus 核心指标 | 是 |
| Tool 调用日志 | tool_logs 表持久化 | 是 |
| Planner（DAG 多步规划） | 复杂任务拆解 | 否（Phase 2） |
| 元数据 / Kafka / Flink Tool | 运维排障工具 | 否（Phase 2） |
| 模型路由 | 按场景选不同模型 | 否（Phase 2） |
| Token 成本优化 | Prompt 压缩、缓存层 | 否（Phase 2/3） |
| 完整 RBAC 权限系统 | 多角色多租户 | 否（Phase 3） |

### 1.3 MVP 目标

| 指标 | 目标 |
|---|---|
| SQL 生成成功率 | > 85% |
| 单次响应延迟 | < 10s（含 LLM + SQL 执行） |
| 并发会话 | 10+ |
| Tool 调用成功率 | > 90% |

---

## 2. 系统架构

### 2.1 架构图

```
┌──────────┐     ┌──────────────┐     ┌──────────────┐     ┌──────────┐
│ 用户输入  │ ──→ │ AI Gateway   │ ──→ │ ReAct Agent  │ ──→ │ 返回结果  │
│ (NL)     │     │ (鉴权/限流)   │     │ (LLM+run_sql) │     │ (Stream) │
└──────────┘     └──────────────┘     └──────┬───────┘     └──────────┘
                                              │
                                    ┌─────────▼─────────┐
                                    │   Redis Memory    │
                                    │ (最近5轮对话)       │
                                    └───────────────────┘
```

### 2.2 数据流

```
1. 用户发送自然语言 query
2. Gateway 鉴权 (JWT) + 注入 trace_id
3. Intent Router 识别意图
4. Agent 进入 ReAct 循环：
   a. Thought: LLM 推理下一步
   b. Action: LLM 生成 Tool Call (run_sql)
   c. Permission: 权限校验 + SQL 安全校验
   d. Observation: 执行 SQL，获取结果
   e. 判断是否完成，最多循环 3 次
5. Agent 总结结果，流式返回最终回答
6. 日志写入 tool_logs 表，上下文写入 Redis
```

### 2.3 MVP 技术栈

| 组件 | 选型                         | 说明 |
|---|----------------------------|---|
| API 框架 | FastAPI (Python)           | 轻量、异步、开发效率高 |
| LLM | 千问                         | MVP 阶段先用单一强模型 |
| Intent 分类 | 规则匹配 + BGE-M3 Embedding    | 冷启动无需训练模型 |
| SQL 解析 | sqlglot (Python)           | 纯 Python，支持多引擎 AST 解析 |
| 缓存/记忆 | Redis                      | 短期对话上下文存储 |
| 数据库 |  PostgreSQL         | 会话和日志持久化 |
| SQL 引擎 | ClickHouse  | MVP 聚焦单引擎 |
| 监控 | Prometheus + Grafana       | 基础指标采集 |

---

## 3. 核心模块设计

### 3.1 AI Gateway

统一入口，MVP 阶段实现：

- **鉴权**：JWT 校验，无效 token 直接拒绝
- **限流**：简单实现，单用户每分钟最多 30 次请求
- **Trace**：为每个请求生成 `trace_id`，贯穿全链路

```python
@app.post("/api/v1/chat")
async def chat(request: ChatRequest):
    trace_id = generate_trace_id()
    validate_jwt(request.token)
    check_rate_limit(request.user_id)
    return handle_chat(request, trace_id)
```

### 3.2 Intent 识别

MVP 采用**两层策略**：

**第一层：规则匹配（覆盖 80%+ 场景）**

| 关键词 | 意图 |
|---|---|
| 查询、SELECT、多少、统计 | `SQL_QUERY` |
| 为什么、异常、下降、问题 | `TROUBLESHOOT`（MVP 阶段返回"暂未支持，敬请期待"） |

**第二层：LLM 兜底**

规则未匹配时，将 query 交给 LLM 判断意图类型。MVP 阶段不引入 Embedding 分类，避免额外的基础设施。

```python
class IntentClassifier:
    def classify(self, query: str) -> str:
        # 规则匹配
        if any(kw in query for kw in ["查询", "多少", "统计", "查一下"]):
            return "SQL_QUERY"
        # LLM 兜底
        return self._llm_classify(query)
```

### 3.3 ReAct 执行引擎

MVP 不实现完整的 DAG Planner，采用简化的 ReAct 循环：

```
┌──────────┐
│ Thought   │ ← LLM 推理：需要做什么？
└─────┬────┘
      ▼
┌──────────┐
│ Action    │ ← 生成 Tool Call (run_sql)
└─────┬────┘
      ▼
┌──────────┐
│ Permission│ ← SQL 安全校验
└─────┬────┘
      ▼
┌───────────┐
│ Observation│ ← 执行 SQL，获取结果
└─────┬─────┘
      ▼
┌──────────┐
│ Done?     │ ← Yes → 输出最终答案
└─────┬────┘
      │ No (继续循环，最多 3 次)
      │
```

#### 状态定义

```python
@dataclass
class ExecutionContext:
    session_id: str
    state: str                      # "planning" | "executing" | "completed" | "failed"
    step_results: list[dict]        # 已完成步骤的结果
    retry_count: int = 0
    max_retries: int = 3
    tool_call_count: int = 0
    max_tool_calls: int = 3         # MVP: 固定最多 3 轮
    conversation_history: list      # 从 Redis 加载
```

#### 错误恢复

| 错误类型 | 策略 |
|---|---|
| SQL 语法错误 | 将错误信息返回给 LLM，让其修正后重试，最多 3 次 |
| 超时 | 返回"查询超时，请缩小查询范围" |
| 权限拒绝 | 返回明确提示，终止循环 |
| 未知错误 | 记录错误日志，返回"系统异常，请稍后重试" |

### 3.4 Tool 系统

#### Tool 基类

```python
class Tool(ABC):
    name: str
    description: str
    input_schema: dict
    permission_tag: str
    timeout: int = 30

    @abstractmethod
    def execute(self, input: dict) -> ToolResult: ...

@dataclass
class ToolResult:
    success: bool
    data: Any
    error: str | None
    latency_ms: float
```

#### Tool 注册中心

```python
class ToolRegistry:
    _tools: dict[str, Tool] = {}

    @classmethod
    def register(cls, tool: Tool):
        cls._tools[tool.name] = tool

    @classmethod
    def get(cls, name: str) -> Tool | None:
        return cls._tools.get(name)

    @classmethod
    def list_all(cls) -> list[dict]:
        return [
            {"name": t.name, "description": t.description, "input_schema": t.input_schema}
            for t in cls._tools.values()
        ]
```

#### MVP Tool: `run_sql`

| 属性 | 值 |
|---|---|
| 描述 | 执行只读 SQL 查询，返回结果集 |
| 输入 | `{"query": "SELECT ...", "limit": 1000}` |
| 权限 | `sql:read` |
| 引擎 | 先接入一个（ClickHouse 或 Presto） |

```python
class RunSqlTool(Tool):
    name = "run_sql"
    description = "Execute read-only SQL queries against data warehouse"
    permission_tag = "sql:read"
    input_schema = {
        "type": "object",
        "properties": {
            "query": {"type": "string", "description": "SQL SELECT statement"},
            "limit": {"type": "integer", "default": 1000, "max": 10000}
        },
        "required": ["query"]
    }
```

### 3.5 Summarizer

MVP 阶段由 LLM 直接总结结果，不单独实现 Summarizer 模块。System Prompt 中约束：

```
你是一个数据平台分析助手。基于工具返回的数据结果，
用简洁自然的方式回答用户问题。要求：
1. 所有结论必须有数据支撑，不编造
2. 先给出核心结论，再补充细节
3. 数据量较大时，用表格或要点形式呈现
```

---

## 4. SQL 安全设计

MVP 阶段安全是核心，必须实现四层防护：

### 4.1 四层防护

| 控制层 | 措施 | 实现方式 |
|---|---|---|
| 账号层 | 只读数据库账号 | 数据库连接使用只读用户 |
| 解析层 | SQL AST 解析，拦截 DDL/DML | sqlglot 解析 + 关键字检查 |
| 白名单层 | 限定可查询的表 | 配置允许查询的表名单 |
| 限制层 | 强制 LIMIT + 超时 Kill | 自动追加 LIMIT，查询超时 30s |

### 4.2 SQL 校验规则

```python
class SqlValidator:
    # DDL/DML 关键字黑名单
    BLOCKED_KEYWORDS = [
        "INSERT", "UPDATE", "DELETE", "DROP", "ALTER",
        "CREATE", "TRUNCATE", "GRANT", "REVOKE"
    ]

    # 允许查询的表名单
    ALLOWED_TABLES: set[str] = set()  # 配置加载

    @classmethod
    def validate(cls, sql: str) -> ValidationResult:
        # 1. 解析 SQL AST
        # 2. 检查 DDL/DML 关键字
        # 3. 表名白名单校验
        # 4. 自动添加 LIMIT（如缺失）
        ...
```

### 4.3 风险规则

| 风险 | 处理 |
|---|---|
| DDL/DML 语句 | 拒绝 |
| 不在白名单中的表 | 拒绝 |
| 无 LIMIT | 自动添加（默认 1000） |
| 全表扫描（无 WHERE 且无 LIMIT） | 拒绝 |
| 查询超时 | 自动 Kill（30s） |

---

## 5. 权限与安全

### 5.1 MVP 权限模型

简化实现：所有通过 Gateway 的合法用户，自动绑定 `sql:read` 权限。不实现完整的 RBAC 系统，但预留权限校验接口。

```python
class PermissionChecker:
    def check(self, user_id: str, tool_name: str) -> bool:
        # MVP: 所有合法用户拥有 sql:read 权限
        if tool_name == "run_sql":
            return True
        return False
```

### 5.2 Prompt Injection 防御

- 系统 Prompt 与用户输入严格隔离
- LLM 只能调用 `ToolRegistry` 中注册的工具
- SQL 必须经过 `SqlValidator` 校验才能执行

---

## 6. 上下文管理

### 6.1 Redis 短期记忆

MVP 只实现短期记忆：在 Redis 中保存最近 5 轮对话。

```
Key:   copilot:{user_id}:{session_id}
Value: [{"role": "user", "content": "..."}, {"role": "assistant", "content": "..."}, ...]
TTL:   30 分钟
```

每次对话时，从 Redis 加载最近 5 轮对话作为 LLM 上下文。

### 6.2 上下文裁剪

- 保留：用户原始问题、最近 5 轮对话、关键 Tool 结果摘要
- 丢弃：中间推理步骤、Tool 完整结果集（只保留摘要）

```python
def summarize_tool_result(result: ToolResult) -> str:
    if isinstance(result.data, list):
        if len(result.data) > 10:
            return f"共 {len(result.data)} 条结果，前 10 条：{result.data[:10]} ..."
    return str(result.data)[:500]
```

---

## 7. 存储设计

### 7.1 会话表 `chat_session`

```sql
CREATE TABLE chat_session (
    session_id      VARCHAR(64)   PRIMARY KEY,
    user_id         VARCHAR(64)   NOT NULL,
    intent_type     VARCHAR(32),
    status          VARCHAR(16)   DEFAULT 'active',
    created_at      TIMESTAMP     DEFAULT CURRENT_TIMESTAMP,
    updated_at      TIMESTAMP     DEFAULT CURRENT_TIMESTAMP,
    context_summary TEXT
);
```

### 7.2 工具调用日志表 `tool_logs`

```sql
CREATE TABLE tool_logs (
    id              BIGINT AUTO_INCREMENT PRIMARY KEY,
    session_id      VARCHAR(64)   NOT NULL,
    trace_id        VARCHAR(64),
    tool_name       VARCHAR(64)   NOT NULL,
    input_params    JSON,
    output_data     JSON,
    status          VARCHAR(16),                    -- success / failed / timeout
    error_message   TEXT,
    latency_ms      INT,
    retry_count     INT           DEFAULT 0,
    created_at      TIMESTAMP     DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_tool_logs_session ON tool_logs(session_id);
CREATE INDEX idx_tool_logs_tool    ON tool_logs(tool_name, created_at);
```

---

## 8. API 设计

### 8.1 Chat API

```
POST /api/v1/chat
```

**Request:**

```json
{
  "session_id": "sess_001",
  "user_id": "user_alice",
  "message": "查询昨天 GMV 总量"
}
```

**Response (Streaming SSE):**

```
event: thinking
data: {"type": "thinking", "content": "正在生成 SQL..."}

event: tool_call
data: {"type": "tool_call", "tool_name": "run_sql", "input": {"query": "SELECT sum(gmv) ..."}}

event: tool_result
data: {"type": "tool_result", "tool_name": "run_sql", "summary": "昨日 GMV: 141万"}

event: final_answer
data: {"type": "final_answer", "content": "昨天 GMV 为 141 万元。"}
```

### 8.2 会话管理接口

| Method | Path | 说明 |
|---|---|---|
| `GET` | `/api/v1/sessions` | 获取用户会话列表 |
| `GET` | `/api/v1/sessions/{id}` | 获取会话详情 |
| `DELETE` | `/api/v1/sessions/{id}` | 删除会话 |

### 8.3 日志接口

| Method | Path | 说明 |
|---|---|---|
| `GET` | `/api/v1/logs?session_id=xxx` | 获取会话的工具调用日志 |

---

## 9. 可观测性

### 9.1 Prometheus 指标

| 指标名 | 说明 |
|---|---|
| `copilot_chat_requests_total` | 聊天请求总数（按 status 区分） |
| `copilot_chat_latency_seconds` | 聊天请求延迟（直方图） |
| `copilot_tool_calls_total` | Tool 调用总数（按 tool_name + status 区分） |
| `copilot_tool_latency_seconds` | Tool 执行延迟（直方图） |
| `copilot_llm_latency_seconds` | LLM 调用延迟 |
| `copilot_sql_success_rate` | SQL 执行成功率 |

### 9.2 日志格式

```json
{
  "timestamp": "2026-05-07T10:30:00Z",
  "level": "INFO",
  "trace_id": "trace_abc123",
  "session_id": "sess_001",
  "component": "executor",
  "event": "tool_executed",
  "tool_name": "run_sql",
  "latency_ms": 1250,
  "status": "success"
}
```

---

## 10. 项目结构

```
copilot-mvp/
├── src/
│   ├── gateway/
│   │   ├── __init__.py
│   │   ├── api.py              # FastAPI 路由
│   │   ├── auth.py             # JWT 鉴权
│   │   └── rate_limiter.py     # 限流
│   ├── agent/
│   │   ├── __init__.py
│   │   ├── intent.py           # 意图识别
│   │   ├── react.py            # ReAct 循环
│   │   └── context.py          # 上下文管理
│   ├── tools/
│   │   ├── __init__.py
│   │   ├── base.py             # Tool 基类 + Registry
│   │   └── run_sql.py          # SQL 执行工具
│   ├── sql/
│   │   ├── __init__.py
│   │   ├── validator.py        # SQL 安全校验
│   │   └── executor.py         # SQL 执行器
│   └── storage/
│       ├── __init__.py
│       ├── redis_client.py     # Redis 客户端
│       └── db.py               # MySQL/PG 客户端
├── tests/
│   ├── test_intent.py
│   ├── test_sql_validator.py
│   └── test_react.py
├── configs/
│   └── tables.yml              # 表白名单配置
├── requirements.txt
└── Dockerfile
```

---

## 11. 实施计划

### 阶段 1.1：基础框架（1-2 周）

- [ ] FastAPI 项目搭建
- [ ] Chat API（非流式）
- [ ] JWT 鉴权
- [ ] `run_sql` 工具（直接执行，无安全校验）
- [ ] 数据库连接

### 阶段 1.2：安全与 Agent（1-2 周）

- [ ] SQL 安全校验（AST 解析 + 白名单 + LIMIT）
- [ ] ReAct 循环（固定 3 轮）
- [ ] Intent 识别（规则 + LLM 兜底）
- [ ] 流式响应（SSE）

### 阶段 1.3：上下文与可观测性（1 周）

- [ ] Redis 短期记忆（最近 5 轮对话）
- [ ] tool_logs 持久化
- [ ] Prometheus 指标采集
- [ ] 基础监控面板（Grafana）

### 阶段 1.4：MVP 验收

- [ ] SQL 生成成功率 > 85%
- [ ] 单次响应延迟 < 10s
- [ ] 并发 10+ 会话无异常
- [ ] 安全测试：注入攻击、DDL/DML 拦截验证

---


