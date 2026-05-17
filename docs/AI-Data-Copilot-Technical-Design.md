# AI Data Copilot 技术实现方案

## 1. 概述

### 1.1 背景与目标

AI Data Copilot 定位为"数据工程师助手"，通过 LLM + Tool 架构，使 AI 具备**查询、分析、执行、决策**四大能力，实现 SQL 自动生成、数据分析自动化、数据 Pipeline 排障辅助，最终将数据开发与排障效率提升 3 倍。

### 1.2 设计原则

- **安全优先** — 所有工具调用前置权限校验，SQL 严格白名单，禁止写操作
- **可追溯** — 全量记录工具调用日志，每次操作可审计
- **可扩展** — 模块化解耦，Tool 可独立注册和扩展
- **防幻觉** — 强制通过工具获取数据，禁止无依据回答

---

## 2. 系统架构

### 2.1 整体流程

```
┌──────────┐     ┌──────────────┐     ┌───────────────┐     ┌──────────────┐     ┌──────────┐
│ 用户输入  │ ──→ │ Intent 识别   │ ──→ │ Planner 规划   │ ──→ │ Executor 执行 │ ──→ │ AI 总结   │
│ (NL)     │     │ (Classifier)  │     │ (Task Graph)  │     │ (ReAct Loop)  │     │ (Render)  │
└──────────┘     └──────────────┘     └───────────────┘     └──────────────┘     └──────────┘
```

### 2.2 系统分层

```
┌──────────────────────────────────────────────────────┐
│                   Presentation Layer                  │
│  Web UI / CLI / API Gateway                          │
├──────────────────────────────────────────────────────┤
│                   Application Layer                   │
│  Session Manager │ Context Manager │ Auth Gateway    │
├──────────────────────────────────────────────────────┤
│                   Agent Layer                         │
│  Intent Classifier │ Planner │ Executor │ Summarizer │
├──────────────────────────────────────────────────────┤
│                   Tool Layer                          │
│  run_sql │ query_metadata │ query_logs │ ...         │
├──────────────────────────────────────────────────────┤
│                   Infrastructure Layer                │
│  DB │ Message Queue │ Cache │ Logging │ Monitoring   │
└──────────────────────────────────────────────────────┘
```

---

## 3. 核心模块设计

### 3.1 Intent 识别模块

#### 3.1.1 功能

将用户自然语言输入分类到预定义意图，决定后续处理路径。

#### 3.1.2 意图分类

| Intent 类型 | 触发场景 | 后续路由 |
|---|---|---|
| `SQL_QUERY` | 用户需要查询数据 | → Planner → SQL 生成 |
| `METRIC_ANALYSIS` | 用户需要分析业务指标 | → Planner → 多步分析 |
| `TROUBLESHOOT` | 用户需要排查 Pipeline 问题 | → Planner → 日志/状态查询 |
| `METADATA_QUERY` | 用户查询表结构、血缘 | → 直接调用 Metadata Tool |

#### 3.1.3 实现方案

**方案 A：小模型分类（推荐）**

- 使用轻量分类模型（如 BERT/RoBERTa fine-tune）
- 输入：用户 query
- 输出：意图分类 + 置信度

**方案 B：Embedding + 相似度匹配**

- 预构建意图样例的 embedding 向量库
- 通过余弦相似度匹配最近意图
- 适合冷启动阶段

```python
class IntentClassifier:
    """意图分类器接口"""

    def classify(self, query: str) -> IntentResult:
        """
        Returns:
            IntentResult: {intent_type, confidence, extracted_params}
        """
        ...
```

#### 3.1.4 兜底策略

- 置信度低于阈值 → 交由 LLM 进行二次判断
- 无法识别 → 返回澄清问题引导用户

---

### 3.2 Planner（任务规划模块）

#### 3.2.1 功能

将用户意图拆解为可执行的有向无环图（DAG）任务序列。

#### 3.2.2 输出结构

```python
@dataclass
class TaskPlan:
    plan_id: str
    intent: str                  # 关联的意图类型
    steps: list[PlanStep]        # 执行步骤（有序 DAG）
    max_retries: int = 3         # 最大重试次数
    timeout_seconds: int = 60    # 整体超时时间

@dataclass
class PlanStep:
    step_id: str
    tool_name: str               # 调用的工具
    depends_on: list[str]        # 前置依赖步骤
    input_template: dict         # 工具输入模板（含变量占位）
    description: str             # 步骤描述（用于 LLM 推理）
```

#### 3.2.3 规划示例

> 用户：为什么今天的 GMV 下降了？

Planner 输出：

```yaml
plan_id: "plan_001"
intent: "METRIC_ANALYSIS"
steps:
  - step_id: "s1"
    tool_name: "run_sql"
    depends_on: []
    input_template:
      query: "SELECT sum(gmv) FROM dws_order WHERE dt = '${today}'"
    description: "查询今日 GMV 总量"

  - step_id: "s2"
    tool_name: "run_sql"
    depends_on: []
    input_template:
      query: "SELECT sum(gmv) FROM dws_order WHERE dt = '${yesterday}'"
    description: "查询昨日 GMV 总量"

  - step_id: "s3"
    tool_name: "run_sql"
    depends_on: ["s1", "s2"]
    input_template:
      query: "SELECT region, sum(gmv) FROM dws_order WHERE dt = '${today}' GROUP BY region"
    description: "按地区维度拆分 GMV"

  - step_id: "s4"
    tool_name: "run_sql"
    depends_on: ["s3"]
    input_template:
      query: "SELECT region, sum(gmv) FROM dws_order WHERE dt = '${yesterday}' GROUP BY region"
    description: "按地区维度拆分昨日 GMV"
```

#### 3.2.4 实现方案

- **方案 A**：LLM 直接生成 Plan（Prompt Engineering + JSON Schema 约束）
- **方案 B**：预定义 Plan 模板 + LLM 参数填充（适合高频场景）
- 混合策略：高频场景走模板，长尾场景走 LLM 生成

---

### 3.3 Tool 系统

#### 3.3.1 Tool 接口规范

```python
class Tool(ABC):
    """所有工具的基类"""

    name: str
    description: str
    input_schema: dict        # JSON Schema 定义
    permission_tag: str       # 权限标签
    timeout: int = 30         # 超时时间（秒）

    @abstractmethod
    def execute(self, input: dict) -> ToolResult:
        """执行工具，返回结构化结果"""
        ...

@dataclass
class ToolResult:
    success: bool
    data: Any                 # 返回数据
    error: str | None         # 错误信息
    latency_ms: float         # 执行耗时
    metadata: dict = None     # 扩展元信息
```

#### 3.3.2 核心 Tool 定义

**Tool 1: `run_sql` — SQL 执行**

| 属性 | 值 |
|---|---|
| 描述 | 执行只读 SQL 查询，返回结果集 |
| 输入 | `{"query": "SELECT ...", "limit": 1000, "engine": "presto"}` |
| 权限 | `sql:read` |
| 安全约束 | 仅允许 SELECT；自动添加 LIMIT；禁止子查询写操作 |

```python
class RunSqlTool(Tool):
    name = "run_sql"
    description = "Execute read-only SQL queries against data warehouse"
    permission_tag = "sql:read"
    input_schema = {
        "type": "object",
        "properties": {
            "query": {"type": "string", "description": "SQL SELECT statement"},
            "limit": {"type": "integer", "default": 1000, "max": 10000},
            "engine": {"type": "string", "enum": ["presto", "clickhouse", "mysql"]}
        },
        "required": ["query"]
    }
```

**Tool 2: `query_metadata` — 元数据查询**

| 属性 | 值 |
|---|---|
| 描述 | 查询表结构、字段来源、数据血缘 |
| 输入 | `{"table": "dws_order", "type": "schema|lineage|source"}` |
| 权限 | `metadata:read` |

能力：
- 查询表 schema（字段名、类型、注释）
- 查询字段数据来源
- 查询表级血缘关系（上游/下游依赖）

**Tool 3: `query_logs` — 日志查询**

| 属性 | 值 |
|---|---|
| 描述 | 查询数据 Pipeline 运行日志与状态 |
| 输入 | `{"job_id": "flink_job_01", "type": "kafka_lag|fllink_status|task_log"}` |
| 权限 | `log:read` |

能力：
- Kafka Consumer Lag 查询
- Flink Job 状态查询
- 数据同步 Task 日志查询

#### 3.3.3 Tool 注册与发现

```python
class ToolRegistry:
    """工具注册中心"""

    _tools: dict[str, Tool] = {}

    @classmethod
    def register(cls, tool: Tool):
        cls._tools[tool.name] = tool

    @classmethod
    def get(cls, name: str) -> Tool | None:
        return cls._tools.get(name)

    @classmethod
    def list_all(cls) -> list[dict]:
        """返回所有工具的元信息（用于 Prompt 构造）"""
        return [
            {"name": t.name, "description": t.description, "input_schema": t.input_schema}
            for t in cls._tools.values()
        ]
```

---

### 3.4 Executor（ReAct 执行引擎）

#### 3.4.1 核心循环

```
                    ┌──────────────────────────────────┐
                    │                                  │
                    ▼                                  │
              ┌──────────┐                             │
              │ Thought   │ ← LLM 推理：下一步做什么？    │
              └─────┬────┘                             │
                    │                                  │
                    ▼                                  │
              ┌──────────┐                             │
              │ Action    │ ← 选择 Tool + 构造参数       │
              └─────┬────┘                             │
                    │                                  │
                    ▼                                  │
              ┌──────────┐                             │
              │ Permission│ ← 权限校验（拦截非法操作）    │
              │  Check    │                             │
              └─────┬────┘                             │
                    │ ✓                                │
                    ▼                                  │
              ┌───────────┐                            │
              │ Observation│ ← 执行 Tool，获取结果       │
              └─────┬─────┘                            │
                    │                                  │
                    ▼                                  │
              ┌──────────┐                             │
              │ Done?     │ ── Yes ──→ 输出最终结果 ───┘
              └─────┬────┘
                    │ No (继续循环)
                    │
```

#### 3.4.2 状态机定义

```python
class ExecutionState(Enum):
    PLANNING = "planning"        # 规划阶段
    EXECUTING = "executing"      # 执行阶段
    WAITING = "waiting"          # 等待前置步骤
    RETRYING = "retrying"        # 重试中
    COMPLETED = "completed"      # 完成
    FAILED = "failed"            # 失败
    ABORTED = "aborted"          # 用户中止

@dataclass
class ExecutionContext:
    session_id: str
    plan: TaskPlan
    state: ExecutionState
    step_results: dict[str, ToolResult]  # 已完成步骤的结果
    current_step: PlanStep | None
    retry_count: int = 0
    max_retries: int = 3
    conversation_history: list[dict]      # 精简后的对话历史
```

#### 3.4.3 错误恢复策略

| 错误类型 | 恢复策略 |
|---|---|
| SQL 语法错误 | LLM 自动修复 SQL，最多重试 3 次 |
| 超时 | 降级 limit，缩小查询范围重试 |
| Tool 不可用 | 尝试备用 Tool（如 presto → clickhouse） |
| 权限拒绝 | 返回明确提示，不重试 |
| 未知错误 | 记录错误上下文，交由 LLM 决策 |

```python
class ErrorRecovery:
    """错误恢复处理器"""

    STRATEGIES = {
        "SQL_SYNTAX_ERROR":  {"action": "auto_fix", "max_retries": 3},
        "TIMEOUT":            {"action": "reduce_limit", "factor": 0.5},
        "TOOL_UNAVAILABLE":   {"action": "fallback_tool", "alternatives": []},
        "PERMISSION_DENIED":  {"action": "abort", "message": "权限不足"},
        "UNKNOWN_ERROR":      {"action": "llm_decide"},
    }

    def recover(self, error: ExecutionError, ctx: ExecutionContext) -> RecoveryAction:
        ...
```

---

### 3.5 Summarizer（结果总结模块）

#### 3.5.1 功能

将工具执行结果转化为用户友好的自然语言总结。

#### 3.5.2 输出模板

- **数据查询类**：返回结构化表格 + 关键数字摘要
- **指标分析类**：趋势描述 + 异常点标注 + 归因分析
- **排障类**：问题定位 + 根因分析 + 建议操作

```python
class Summarizer:
    """结果总结器"""

    SYSTEM_PROMPT = """你是一个数据平台分析助手。基于工具返回的数据结果，
    用简洁自然的方式回答用户问题。要求：
    1. 所有结论必须有数据支撑，不编造
    2. 先给出核心结论，再补充细节
    3. 数据量较大时，用表格或要点形式呈现
    4. 指出数据中的异常点和关键趋势"""
```

---

## 4. 权限与安全设计

### 4.1 权限模型

```
User ──→ Role ──→ Permission Set ──→ Tool Tags
```

- 用户关联一个或多个角色
- 角色绑定一组权限标签
- 每个 Tool 声明所需权限标签
- 执行前校验：用户权限集 ∩ Tool 所需标签 ≠ ∅

### 4.2 SQL 安全控制

| 控制层 | 措施 |
|---|---|
| 账号层 | 使用只读数据库账号，无写入权限 |
| 解析层 | SQL AST 解析，拦截 DDL/DML 语句 |
| 白名单层 | 限定可查询的表/字段白名单 |
| 限制层 | 强制添加 LIMIT（默认 1000，最大 10000） |
| 超时层 | 查询超时自动 Kill（默认 30s） |

```python
class SqlValidator:
    """SQL 安全校验"""

    # DDL/DML 关键字黑名单
    BLOCKED_KEYWORDS = [
        "INSERT", "UPDATE", "DELETE", "DROP", "ALTER",
        "CREATE", "TRUNCATE", "GRANT", "REVOKE"
    ]

    # 允许查询的表白名单
    ALLOWED_TABLES: set[str] = set()  # 动态加载

    @classmethod
    def validate(cls, sql: str) -> ValidationResult:
        # 1. AST 解析检查
        # 2. 关键字匹配
        # 3. 表名白名单校验
        ...
```

### 4.3 调用前拦截链

```
User Request
    ↓
┌──────────────┐
│ Auth Filter   │ ← 用户身份认证
└──────┬───────┘
       ↓
┌──────────────┐
│ Intent Filter │ ← 意图合法性检查
└──────┬───────┘
       ↓
┌──────────────┐
│ Permission    │ ← 权限标签匹配
│  Check        │
└──────┬───────┘
       ↓
┌──────────────┐
│ Sql Validator │ ← SQL 安全校验（仅 run_sql）
└──────┬───────┘
       ↓
    Execute Tool
```

---

## 5. 上下文管理

### 5.1 上下文窗口策略

LLM 上下文有限，需进行智能裁剪：

| 保留内容 | 丢弃内容 |
|---|---|
| 用户原始问题 | 中间推理步骤的详细输出 |
| Plan 任务列表 | Tool 返回的完整结果集（保留摘要） |
| 关键中间结果 | 失败的尝试记录（保留错误类型） |
| 最近 3 轮对话 | 早期对话历史 |

### 5.2 结果摘要机制

```python
def summarize_tool_result(result: ToolResult) -> str:
    """将 Tool 结果压缩为简短摘要"""
    if isinstance(result.data, list):
        if len(result.data) > 10:
            return f"共 {len(result.data)} 条结果，前 10 条：{result.data[:10]} ..."
    return str(result.data)[:500]  # 限制 500 字符
```

### 5.3 会话状态持久化

长会话中，将上下文持久化到存储层，避免内存膨胀：

```python
class ContextManager:
    def save_context(self, session_id: str, ctx: ExecutionContext):
        """将执行上下文序列化到数据库"""
        ...

    def load_context(self, session_id: str) -> ExecutionContext:
        """从数据库恢复上下文"""
        ...
```

---

## 6. 存储设计

### 6.1 数据库表结构

#### 6.1.1 会话表 `chat_session`

```sql
CREATE TABLE chat_session (
    session_id      VARCHAR(64)   PRIMARY KEY,
    user_id         VARCHAR(64)   NOT NULL,
    intent_type     VARCHAR(32),                  -- 识别的意图类型
    plan_id         VARCHAR(64),                  -- 关联的任务规划 ID
    status          VARCHAR(16)   DEFAULT 'active',
    created_at      TIMESTAMP     DEFAULT CURRENT_TIMESTAMP,
    updated_at      TIMESTAMP     DEFAULT CURRENT_TIMESTAMP,
    context_summary TEXT                          -- 精简后的上下文摘要
);
```

#### 6.1.2 工具调用日志表 `tool_logs`

```sql
CREATE TABLE tool_logs (
    id              BIGINT AUTO_INCREMENT PRIMARY KEY,
    session_id      VARCHAR(64)   NOT NULL,         -- 关联会话
    plan_id         VARCHAR(64),                    -- 关联规划
    step_id         VARCHAR(64),                    -- 关联步骤
    tool_name       VARCHAR(64)   NOT NULL,
    input_params    JSON,                           -- 工具输入
    output_data     JSON,                           -- 工具输出（可能截断）
    status          VARCHAR(16),                    -- success / failed / timeout
    error_message   TEXT,
    latency_ms      INT,                            -- 执行耗时（毫秒）
    retry_count     INT           DEFAULT 0,
    created_at      TIMESTAMP     DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_tool_logs_session ON tool_logs(session_id);
CREATE INDEX idx_tool_logs_tool    ON tool_logs(tool_name, created_at);
```

#### 6.1.3 权限表 `user_permissions`（可选）

```sql
CREATE TABLE user_permissions (
    id              BIGINT AUTO_INCREMENT PRIMARY KEY,
    user_id         VARCHAR(64)   NOT NULL,
    role            VARCHAR(32)   NOT NULL,
    permissions     JSON,                           -- 权限标签列表
    created_at      TIMESTAMP     DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uk_user_role (user_id, role)
);
```

---

## 7. API 设计

### 7.1 对话接口

#### `POST /api/v1/chat`

发起一轮对话。

**Request:**

```json
{
  "session_id": "sess_001",
  "user_id": "user_alice",
  "message": "为什么今天 GMV 下降了？"
}
```

**Response (Streaming):**

```json
{
  "type": "thinking",
  "content": "正在分析 GMV 下降原因..."
}
```

```json
{
  "type": "tool_call",
  "tool_name": "run_sql",
  "input": {"query": "SELECT sum(gmv) ..."}
}
```

```json
{
  "type": "tool_result",
  "tool_name": "run_sql",
  "summary": "今日 GMV: 120万，较昨日下降 15%"
}
```

```json
{
  "type": "final_answer",
  "content": "今日 GMV 为 120 万元，较昨日（141 万元）下降 15%。主要下降来自华东地区..."
}
```

### 7.2 会话管理接口

| Method | Path | 说明 |
|---|---|---|
| `GET` | `/api/v1/sessions` | 获取用户会话列表 |
| `GET` | `/api/v1/sessions/{id}` | 获取会话详情（含对话历史） |
| `DELETE` | `/api/v1/sessions/{id}` | 删除会话 |
| `POST` | `/api/v1/sessions/{id}/abort` | 中止正在执行的任务 |

### 7.3 工具日志接口

| Method | Path | 说明 |
|---|---|---|
| `GET` | `/api/v1/logs?session_id=xxx` | 获取会话的工具调用日志 |
| `GET` | `/api/v1/logs/stats` | 获取工具调用统计（成功率、延迟分布） |

---

## 8. 监控与可观测性

### 8.1 关键指标

| 指标 | 说明 |
|---|---|
| Intent 分类准确率 | 意图识别正确的比例 |
| Plan 执行成功率 | 任务规划成功完成的比例 |
| Tool 调用成功率 | 各工具调用成功/失败比例 |
| 平均延迟 | 从用户输入到最终回答的端到端延迟 |
| SQL 自动修复率 | SQL 失败后自动修复成功的比例 |
| 用户满意度 | 用户反馈评分 |

### 8.2 日志规范

```json
{
  "timestamp": "2026-05-07T10:30:00Z",
  "level": "INFO",
  "session_id": "sess_001",
  "component": "executor",
  "event": "tool_executed",
  "tool_name": "run_sql",
  "latency_ms": 1250,
  "status": "success",
  "step_id": "s1"
}
```

---

## 9. 技术栈建议

| 组件 | 推荐技术 | 说明 |
|---|---|---|
| LLM | Claude / GPT-4 / 国内大模型 | Planner + Executor + Summarizer 推理 |
| 意图分类 | BERT / FastText | 轻量分类模型 |
| Web 框架 | FastAPI (Python) / Spring Boot (Java) | RESTful API |
| 数据库 | MySQL / PostgreSQL | 会话和日志存储 |
| 缓存 | Redis | 上下文缓存、会话状态 |
| SQL 引擎 | Presto / ClickHouse | 数据查询 |
| 消息队列 | Kafka | 异步任务、事件流 |
| 监控 | Prometheus + Grafana | 指标采集与可视化 |

---

## 10. 风险与应对

| 风险 | 影响 | 应对措施 |
|---|---|---|
| LLM 生成不安全 SQL | 数据泄露 | 多层校验：AST 解析 + 关键字拦截 + 只读账号 |
| 工具调用超时 | 用户体验差 | 超时自动降级（缩小查询范围） |
| 上下文丢失 | 多轮对话质量下降 | 关键结果摘要持久化 |
| 意图分类错误 | 执行路径错误 | 低置信度时 LLM 兜底 + 用户确认 |
| 幻觉回答 | 数据不准确 | 强制工具调用 + 结果与结论一致性校验 |

---

## 11. 实施路线图

### Phase 1：MVP（最小可用）

- 实现 `run_sql` 单一工具
- 基础 Intent 识别（规则匹配）
- 简单 ReAct 循环（固定 3 轮）
- SQL 安全校验（只读 + LIMIT）

### Phase 2：能力扩展

- 接入 `query_metadata`、`query_logs` 工具
- LLM 驱动的 Planner
- 多轮上下文管理
- 错误自动恢复

### Phase 3：生产就绪

- 完整权限系统（RBAC）
- 监控与告警
- 会话持久化与恢复
- 性能优化与压测
