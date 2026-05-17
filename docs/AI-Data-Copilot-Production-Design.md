# AI Data Copilot 生产级技术方案设计

## 1. 概述

### 1.1 背景与目标

AI Data Copilot 定位为"数据工程师助手"，通过 LLM + Tool 架构，使 AI 具备**查询、分析、执行、决策**四大能力，实现 SQL 自动生成、数据分析自动化、数据 Pipeline 排障辅助，最终将数据开发与排障效率提升 3 倍。

### 1.2 设计原则

- **安全优先** — 所有工具调用前置权限校验，SQL 严格白名单，禁止写操作
- **可追溯** — 全量记录工具调用日志，每次操作可审计，全链路 Trace
- **可扩展** — 模块化解耦，Tool 可独立注册和扩展
- **防幻觉** — 强制通过工具获取数据，禁止无依据回答
- **成本可控** — 模型路由 + 缓存 + Prompt 压缩，目标 Token 成本降低 60%

### 1.3 非功能目标

| 指标 | 目标 |
|---|---|
| Query Latency | < 5s |
| SQL 生成成功率 | > 90% |
| Tool 调用成功率 | > 95% |
| 并发会话 | 100+ |
| Token 成本优化 | 降低 60% |
| 可观测性 | 全链路 Trace |

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
┌──────────────────────────────────────────────────────────────┐
│                   Presentation Layer                          │
│  Web UI (React + NextJS) / CLI / API Gateway                 │
├──────────────────────────────────────────────────────────────┤
│                   Application Layer                           │
│  Session Manager │ Context Manager │ Auth Gateway │ Rate Limit│
├──────────────────────────────────────────────────────────────┤
│                   Agent Layer                                 │
│  Intent Classifier │ Planner (LangGraph) │ Executor │ Router  │
├──────────────────────────────────────────────────────────────┤
│                   Tool Layer                                  │
│  run_sql │ query_metadata │ kafka │ flink │ log │ alert       │
├──────────────────────────────────────────────────────────────┤
│                   Infrastructure Layer                        │
│  ClickHouse │ Hive │ Kafka │ Flink │ Neo4j │ Redis │ Prometheus│
└──────────────────────────────────────────────────────────────┘
```

### 2.3 架构数据流

```
                        ┌────────────────────┐
                        │      Web UI        │
                        └─────────┬──────────┘
                                  │
                                  ▼
                  ┌─────────────────────────────┐
                  │      AI Gateway API          │
                  │  (鉴权 / 限流 / Trace 注入)    │
                  └────────────┬────────────────┘
                               │
          ┌────────────────────┼────────────────────┐
          ▼                    ▼                    ▼
 ┌────────────────┐  ┌────────────────┐  ┌────────────────┐
 │ Intent Router  │  │ Session Memory │  │ Permission ACL │
 └──────┬─────────┘  └────────────────┘  └────────────────┘
        │
        ▼
 ┌──────────────────────────────┐
 │         Planner Agent         │
 │      (LangGraph / ReAct)      │
 └──────────────┬───────────────┘
                │
      ┌─────────┴─────────┐
      ▼                   ▼
┌──────────────┐   ┌──────────────┐
│ Tool Executor │   │ LLM Router   │
└──────┬───────┘   └──────────────┘
       │
       ▼
┌──────────────────────────────────────────┐
│                Tool Layer                │
├──────────────────────────────────────────┤
│ SQL Tool │ Metadata │ Kafka │ Flink      │
│ Log Tool │ Alert    │                    │
└──────────────────────────────────────────┘
       │
       ▼
┌──────────────────────────────────────────┐
│          Data Infrastructure             │
├──────────────────────────────────────────┤
│ ClickHouse │ Hive │ Kafka │ Flink        │
│ Redis │ Neo4j │ Prometheus               │
└──────────────────────────────────────────┘
```

---

## 3. 核心模块设计

### 3.1 AI Gateway

#### 职责

统一入口，负责：
- Chat 请求路由与分发
- Token 用量控制
- 用户鉴权（JWT）
- 限流（Redis + Lua）
- Trace ID 注入（OpenTelemetry）

#### 技术选型

| 模块 | 技术 |
|---|---|
| API Framework | SpringBoot |
| API Gateway | Spring Cloud Gateway |
| 鉴权 | JWT |
| 限流 | Redis + Lua |
| Trace | OpenTelemetry |

#### API 设计

**Chat API**

```
POST /api/v1/chat
```

Request:

```json
{
  "session_id": "sess_001",
  "user_id": "user_alice",
  "message": "为什么今天 GMV 下降了？"
}
```

完整 Streaming Response 见 [第 9 节 API 设计](#9-api-设计)。

---

### 3.2 Intent 识别模块

#### 功能

将用户自然语言输入分类到预定义意图，决定后续处理路径。

#### 意图分类

| Intent 类型 | 触发场景 | 后续路由 |
|---|---|---|
| `SQL_QUERY` | 用户需要查询数据 | → Planner → SQL 生成 |
| `METRIC_ANALYSIS` | 用户需要分析业务指标 | → Planner → 多步分析 |
| `TROUBLESHOOT` | 用户需要排查 Pipeline 问题 | → Planner → 日志/状态查询 |
| `METADATA_QUERY` | 用户查询表结构、血缘 | → 直接调用 Metadata Tool |
| `FLINK_JOB` | 用户需要编写 Flink Job | → Planner → 代码生成 |

#### 实现方案

采用**分层分类策略**：

**第一层：规则分类（快速路径）**

通过关键词快速匹配高频意图：
```
为什么、异常、下降 → ANALYSIS
来源、血缘 → LINEAGE / METADATA_QUERY
写 SQL、查询 → SQL_QUERY
```

**第二层：Embedding 相似度匹配**

- 使用 BGE-M3 / Qwen Embedding
- 预构建意图样例的 embedding 向量库
- 通过余弦相似度匹配最近意图
- 适合冷启动阶段和规则未覆盖的 query

**第三层：LLM 兜底**

置信度低于阈值时，交由 LLM 进行二次判断。

```python
class IntentClassifier:
    """意图分类器接口"""

    def classify(self, query: str) -> IntentResult:
        """
        Returns:
            IntentResult: {intent_type, confidence, extracted_params}
        """
        # 1. 规则匹配
        # 2. Embedding 相似度
        # 3. LLM 兜底
        ...
```

#### 输出格式

```json
{
  "intent": "METRIC_ANALYSIS",
  "confidence": 0.91,
  "extracted_params": {"metric": "gmv", "date": "today"}
}
```

---

### 3.3 Planner（任务规划模块）

#### 功能

将用户意图拆解为可执行的有向无环图（DAG）任务序列。

#### 输出结构

```python
@dataclass
class TaskPlan:
    plan_id: str
    intent: str                  # 关联的意图类型
    steps: list[PlanStep]        # 执行步骤（有序 DAG）
    max_retries: int = 3         # 最大重试次数
    max_depth: int = 5           # 最大规划深度，防止 Tool 风暴
    timeout_seconds: int = 60    # 整体超时时间

@dataclass
class PlanStep:
    step_id: str
    tool_name: str               # 调用的工具
    depends_on: list[str]        # 前置依赖步骤
    input_template: dict         # 工具输入模板（含变量占位）
    description: str             # 步骤描述（用于 LLM 推理）
```

#### 规划示例

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

#### 实现方案

**为什么选择 LangGraph：**

| LangChain | LangGraph |
|---|---|
| 顺序链 | 支持状态机 |
| 难处理循环 | 支持 ReAct 循环 |
| 状态弱 | 强状态管理 |
| 不适合复杂 Agent | 更适合企业 Agent |

- **方案 A**：LLM 直接生成 Plan（Prompt Engineering + JSON Schema 约束）
- **方案 B**：预定义 Plan 模板 + LLM 参数填充（适合高频场景）
- **混合策略**：高频场景走模板，长尾场景走 LLM 生成

#### Graph State 设计

```python
class AgentState(TypedDict):
    user_query: str
    intent: str
    current_step: int
    plan: list
    observations: list
    tool_results: list
    final_answer: str
    retry_count: int
    tool_call_count: int       # 累计 Tool 调用次数，用于风暴防护
```

---

### 3.4 Tool 系统

#### Tool 接口规范

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

#### Tool 生命周期

```
LLM 生成 Tool Call
        ↓
    参数校验
        ↓
    权限校验（ACL + RBAC）
        ↓
    执行 Tool
        ↓
    结果标准化
        ↓
    返回 Agent
```

#### Tool 注册与发现

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

#### 核心 Tool 定义

**Tool 1: `run_sql` — SQL 执行**

| 属性 | 值 |
|---|---|
| 描述 | 执行只读 SQL 查询，返回结果集 + Explain 分析 |
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

**SQL 安全控制（四层防护）：**

| 控制层 | 措施 |
|---|---|
| 账号层 | 使用只读数据库账号，无写入权限 |
| 解析层 | SQL AST 解析（JSqlParser / Calcite），拦截 DDL/DML 语句 |
| 白名单层 | 限定可查询的表/字段白名单 |
| 限制层 | 强制添加 LIMIT（默认 1000，最大 10000），查询超时自动 Kill（默认 30s） |

**SQL 风险规则：**

| 风险 | 处理 |
|---|---|
| 全表扫描（无 WHERE） | 拒绝 |
| 无 LIMIT | 自动补充 |
| SELECT * | 告警并提示具体字段 |
| 大时间范围查询 | 拒绝，提示缩小范围 |
| DDL/DML 语句（INSERT/UPDATE/DELETE/DROP/ALTER/TRUNCATE） | 拒绝 |

**SQL 自动修复：**

当 SQL 执行失败时，将错误上下文交给 LLM 自动修复：

```json
// 输入
{"sql": "SELECT xxx FROM orders", "error": "column xxx not found"}

// 输出
{"fixed_sql": "SELECT order_id FROM orders"}
```

自动修复最多重试 3 次。

---

**Tool 2: `query_metadata` — 元数据查询**

| 属性 | 值 |
|---|---|
| 描述 | 查询表结构、字段来源、数据血缘 |
| 输入 | `{"table": "dws_order", "type": "schema|lineage|source"}` |
| 权限 | `metadata:read` |

**数据来源：** Hive Metastore / Atlas / DataHub / 自建元数据系统

**存储方案：** 使用 Neo4j 图数据库存储血缘关系

- 节点：Table、Column、Job、Topic
- 关系：DEPENDS_ON、PRODUCES、CONSUMES

```cypher
MATCH (a:Table {name:'orders'})-[:DEPENDS_ON*1..3]->(b)
RETURN b
```

**能力：**
- 查询表 schema（字段名、类型、注释）
- 查询字段数据来源
- 查询表级血缘关系（上游/下游依赖）
- Schema 变更历史

---

**Tool 3: `query_logs` — 日志查询**

| 属性 | 值 |
|---|---|
| 描述 | 查询数据 Pipeline 运行日志与状态 |
| 输入 | `{"job_id": "flink_job_01", "type": "kafka_lag|flink_status|task_log"}` |
| 权限 | `log:read` |

---

**Tool 4: `kafka` — Kafka 运维**

| 功能 | 数据来源 |
|---|---|
| 查看 Consumer Lag | Kafka Admin API / Burrow |
| 查看 Topic TPS | Prometheus |
| 查看消费组状态 | Kafka Admin API |
| 查看分区倾斜 | Prometheus |

```json
{
  "topic": "orders",
  "lag": 120000,
  "consumer_status": "slow"
}
```

---

**Tool 5: `flink` — Flink 运维**

| 功能 | 数据来源 |
|---|---|
| 查看 Job 状态 | Flink REST API |
| 查看 Backpressure | Prometheus |
| 查看 Checkpoint 延迟 | Flink REST API |
| 查看 Watermark | Flink REST API |

```json
{
  "job": "order_job",
  "checkpoint_delay": "120s",
  "backpressure": "HIGH"
}
```

---

**Tool 6: `alert` — 告警分析**

| 功能 | 数据来源 |
|---|---|
| 查询活跃告警 | Prometheus AlertManager |
| 告警历史 | 自建告警系统 |
| 告警趋势 | Prometheus |

---

### 3.5 Executor（ReAct 执行引擎）

#### 核心循环

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

#### 状态机定义

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
    tool_call_count: int = 0     # Tool 调用计数
    max_tool_calls: int = 10     # 单次会话最大 Tool 调用次数
    conversation_history: list[dict]      # 精简后的对话历史
```

#### 错误恢复策略

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

### 3.6 Summarizer（结果总结模块）

#### 功能

将工具执行结果转化为用户友好的自然语言总结。

#### 输出模板

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

## 4. 模型路由设计

### 4.1 为什么需要模型路由

不同任务对模型能力的需求差异很大。使用统一的大模型会导致：
- 简单任务浪费 Token 成本
- 复杂任务可能能力不足
- 无法平衡延迟与质量

通过模型路由，按场景选择最合适的模型，目标降低 60% Token 成本。

### 4.2 路由策略

| 场景 | 模型 | 选择理由 |
|---|---|---|
| 意图分类（简单文本分类） | 小模型 / 本地 Embedding | 低成本、低延迟 |
| SQL 生成 | DeepSeek-V3 / 同级模型 | 代码能力强，性价比高 |
| 根因分析 | GPT-4o | 推理质量高 |
| 复杂多步推理 / Planner | Claude | 长上下文理解能力强 |
| 结果总结 | 小模型 | 纯文本生成，无需强推理 |

### 4.3 Router 实现

根据以下输入特征动态选择模型：

- **Query 长度**：短 query 倾向小模型
- **计划 Tool 数量**：Tool 调用越多，需要越强模型
- **是否需要推理**：根因分析等需要深度推理的场景用强模型
- **历史失败率**：某模型连续失败时自动切换

```python
@dataclass
class ModelRouteConfig:
    model: str
    max_tokens: int
    temperature: float

class ModelRouter:
    """模型路由器"""

    def route(self, query: str, intent: str, estimated_complexity: int) -> ModelRouteConfig:
        # 根据意图和复杂度选择模型
        ...
```

---

## 5. 缓存设计

### 5.1 缓存层次

| 缓存类型 | 缓存内容 | 存储 | TTL |
|---|---|---|---|
| Query Cache | SQL 查询结果（相同 SQL 不重复执行） | Redis | 5min |
| Retrieval Cache | 元数据 / Schema / 血缘查询结果 | Redis | 30min |
| LLM Cache | 相同 Prompt 的 LLM 回答 | Redis / 本地 | 1h |
| Embedding Cache | Query 的 embedding 向量，避免重复计算 | Redis / Milvus | 24h |

### 5.2 缓存策略

**优先缓存检索结果，而非最终回答：**

```
缓存：元数据、Schema、血缘关系  ← 变化慢，命中率高
不缓存：最终分析结论              ← 数据实时变化，结论易过期
```

原因：
- 数据会变化，缓存最终回答可能返回过期结论
- 检索结果（如表结构、字段血缘）变化频率低，缓存价值高

### 5.3 缓存 Key 设计

```
copilot:{tenant_id}:{user_id}:{session_id}:{cache_type}:{hash}
```

示例：
```
copilot:companyA:u100:s200:metadata:abc123
```

### 5.4 缓存失效

- SQL 结果缓存：检测到对应表有写入操作时失效
- 元数据缓存：Schema 变更事件触发失效
- LLM 缓存：模型版本更新时全量失效

---

## 6. 权限与安全设计

### 6.1 权限模型

```
User ──→ Role ──→ Permission Set ──→ Tool Tags
```

- 用户关联一个或多个角色
- 角色绑定一组权限标签
- 每个 Tool 声明所需权限标签
- 执行前校验：用户权限集 ∩ Tool 所需标签 ≠ ∅

#### RBAC 角色定义

| 角色 | 权限 |
|---|---|
| Analyst | SELECT、Metadata Read |
| Engineer | SELECT、Metadata Read、Log Read、Kafka/Flink Tool |
| Admin | ALL |
| AI_AGENT | READ_ONLY（系统内部角色，AI 默认权限） |

### 6.2 SQL 安全控制

| 控制层 | 措施 |
|---|---|
| 账号层 | 使用只读数据库账号，无写入权限 |
| 解析层 | SQL AST 解析，拦截 DDL/DML 语句 |
| 白名单层 | 限定可查询的表/字段白名单 |
| 限制层 | 强制添加 LIMIT（默认 1000，最大 10000），查询超时自动 Kill（默认 30s） |

#### Tool ACL

```json
{
  "tool": "run_sql",
  "allow": ["SELECT"],
  "deny": ["DELETE", "DROP", "UPDATE", "INSERT", "ALTER", "TRUNCATE"]
}
```

### 6.3 调用前拦截链

```
User Request
    ↓
┌──────────────┐
│ Auth Filter   │ ← 用户身份认证（JWT）
└──────┬───────┘
       ↓
┌──────────────┐
│ Rate Limiter  │ ← 限流（Redis + Lua）
└──────┬───────┘
       ↓
┌──────────────┐
│ Intent Filter │ ← 意图合法性检查
└──────┬───────┘
       ↓
┌──────────────┐
│ Permission    │ ← 权限标签匹配（RBAC + ACL）
│  Check        │
└──────┬───────┘
       ↓
┌──────────────┐
│ Sql Validator │ ← SQL 安全校验（AST + 白名单 + 规则）
└──────┬───────┘
       ↓
    Execute Tool
```

### 6.4 Prompt Injection 防御

| 风险 | 防御方案 |
|---|---|
| 用户输入覆盖系统指令 | 系统 Prompt 与用户输入严格隔离，用户输入作为独立 message role 传入 |
| 非法 Tool 调用 | Tool 白名单机制，LLM 只能调用注册的工具 |
| 危险 SQL 注入 | SQL AST 分析 + 只读账号 + 表白名单 |
| 高风险操作 | 人工确认机制（如批量数据导出） |

---

## 7. Token 成本优化

### 7.1 优化策略

| 策略 | 说明 | 预期效果 |
|---|---|---|
| 模型路由 | 简单任务走小模型 | 降低 30-40% |
| Prompt 压缩 | 去除冗余上下文，精简 Tool 描述 | 降低 15-20% |
| Summary Memory | 历史对话压缩为摘要，而非全量传入 | 降低 20-30% |
| Retrieval Cache | 缓存元数据/Schema 查询，减少重复 LLM 调用 | 降低 10-15% |
| Tool 结果截断 | 工具返回结果只保留摘要传入上下文 | 降低 10-20% |

综合预期降低 60% Token 成本。

### 7.2 上下文窗口策略

LLM 上下文有限，需进行智能裁剪：

| 保留内容 | 丢弃内容 |
|---|---|
| 用户原始问题 | 中间推理步骤的详细输出 |
| Plan 任务列表 | Tool 返回的完整结果集（保留摘要） |
| 关键中间结果 | 失败的尝试记录（保留错误类型） |
| 最近 3 轮对话 | 早期对话历史 |

### 7.3 结果摘要机制

```python
def summarize_tool_result(result: ToolResult) -> str:
    """将 Tool 结果压缩为简短摘要"""
    if isinstance(result.data, list):
        if len(result.data) > 10:
            return f"共 {len(result.data)} 条结果，前 10 条：{result.data[:10]} ..."
    return str(result.data)[:500]  # 限制 500 字符
```

---

## 8. Tool 调用风暴防护

### 8.1 风险

Agent 可能因推理循环或规划失误，导致短时间内大量调用 Tool，造成：
- 数据库查询压力剧增
- Token 成本失控
- 系统响应延迟

### 8.2 防护措施

| 防护层 | 机制 | 配置 |
|---|---|---|
| 次数限制 | 单次会话最大 Tool 调用次数 | 默认 10 次 |
| 深度限制 | Planner 最大规划深度（DAG 最大层级） | 默认 5 层 |
| 速率限制 | 单位时间内 Tool 调用频率上限 | 默认 5 次/分钟 |
| 熔断机制 | 同一 Tool 连续失败 N 次后暂时熔断 | 连续 3 次失败，熔断 5 分钟 |
| 超时保护 | 整体执行超时后强制终止 | 默认 60 秒 |

```python
class ToolCircuitBreaker:
    """Tool 熔断器"""

    def __init__(self, failure_threshold: int = 3, cooldown_seconds: int = 300):
        self.failure_counts: dict[str, int] = {}
        self.circuit_opened_at: dict[str, datetime] = {}

    def record_failure(self, tool_name: str):
        ...

    def is_available(self, tool_name: str) -> bool:
        """检查 Tool 是否处于熔断状态"""
        ...
```

---

## 9. 上下文管理

### 9.1 Memory 分层设计

| 层级 | 存储 | 内容 | 容量 |
|---|---|---|---|
| 短期记忆 | Redis | 最近 5 轮对话 | 滚动窗口 |
| 摘要记忆 | Redis / DB | 历史对话压缩摘要 | 每 10 轮压缩一次 |
| 长期记忆 | 向量库（Milvus） | 用户偏好、常见问题、常用 SQL | 持久化 |

### 9.2 Redis Key 设计

```
copilot:{tenant_id}:{user_id}:{session_id}
```

示例：
```
copilot:companyA:u100:s200
```

### 9.3 会话状态持久化

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

## 10. 存储设计

### 10.1 数据库表结构

#### 10.1.1 会话表 `chat_session`

```sql
CREATE TABLE chat_session (
    session_id      VARCHAR(64)   PRIMARY KEY,
    user_id         VARCHAR(64)   NOT NULL,
    tenant_id       VARCHAR(64)   NOT NULL,
    intent_type     VARCHAR(32),                  -- 识别的意图类型
    plan_id         VARCHAR(64),                  -- 关联的任务规划 ID
    status          VARCHAR(16)   DEFAULT 'active',
    created_at      TIMESTAMP     DEFAULT CURRENT_TIMESTAMP,
    updated_at      TIMESTAMP     DEFAULT CURRENT_TIMESTAMP,
    context_summary TEXT                          -- 精简后的上下文摘要
);
```

> 注：生产环境建议使用 MySQL/PostgreSQL 存储业务数据。若使用 ClickHouse 存储日志类数据（如下文 tool_logs），可采用 MergeTree 引擎。

#### 10.1.2 工具调用日志表 `tool_logs`

```sql
CREATE TABLE tool_logs (
    id              BIGINT AUTO_INCREMENT PRIMARY KEY,
    session_id      VARCHAR(64)   NOT NULL,         -- 关联会话
    trace_id        VARCHAR(64),                    -- 全链路 Trace ID
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
CREATE INDEX idx_tool_logs_trace   ON tool_logs(trace_id);
```

> ClickHouse 版本（适合日志存储）：
>
> ```sql
> CREATE TABLE tool_logs (
>     trace_id String,
>     session_id String,
>     tool_name String,
>     input String,
>     output String,
>     latency_ms UInt32,
>     status String,
>     create_time DateTime
> ) ENGINE = MergeTree
> ORDER BY (create_time, tool_name);
> ```

#### 10.1.3 SQL 历史表 `sql_history`

```sql
CREATE TABLE sql_history (
    query_id        VARCHAR(64)   PRIMARY KEY,
    user_id         VARCHAR(64)   NOT NULL,
    session_id      VARCHAR(64),
    sql_text        TEXT,
    execute_time    INT,                          -- 执行耗时（毫秒）
    status          VARCHAR(16),                  -- success / failed
    created_at      TIMESTAMP     DEFAULT CURRENT_TIMESTAMP
);
```

> ClickHouse 版本：
>
> ```sql
> CREATE TABLE sql_history (
>     query_id String,
>     user_id String,
>     sql_text String,
>     execute_time UInt32,
>     status String,
>     create_time DateTime
> ) ENGINE = MergeTree
> ORDER BY create_time;
> ```

#### 10.1.4 权限表 `user_permissions`（可选）

```sql
CREATE TABLE user_permissions (
    id              BIGINT AUTO_INCREMENT PRIMARY KEY,
    user_id         VARCHAR(64)   NOT NULL,
    tenant_id       VARCHAR(64)   NOT NULL,
    role            VARCHAR(32)   NOT NULL,
    permissions     JSON,                           -- 权限标签列表
    created_at      TIMESTAMP     DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uk_user_role (user_id, tenant_id, role)
);
```

---

## 11. API 设计

### 11.1 对话接口

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
{"type": "thinking", "content": "正在分析 GMV 下降原因..."}
```

```json
{"type": "tool_call", "tool_name": "run_sql", "input": {"query": "SELECT sum(gmv) ..."}}
```

```json
{"type": "tool_result", "tool_name": "run_sql", "summary": "今日 GMV: 120万，较昨日下降 15%"}
```

```json
{"type": "final_answer", "content": "今日 GMV 为 120 万元，较昨日（141 万元）下降 15%。主要下降来自华东地区..."}
```

### 11.2 会话管理接口

| Method | Path | 说明 |
|---|---|---|
| `GET` | `/api/v1/sessions` | 获取用户会话列表 |
| `GET` | `/api/v1/sessions/{id}` | 获取会话详情（含对话历史） |
| `DELETE` | `/api/v1/sessions/{id}` | 删除会话 |
| `POST` | `/api/v1/sessions/{id}/abort` | 中止正在执行的任务 |

### 11.3 工具日志接口

| Method | Path | 说明 |
|---|---|---|
| `GET` | `/api/v1/logs?session_id=xxx` | 获取会话的工具调用日志 |
| `GET` | `/api/v1/logs/stats` | 获取工具调用统计（成功率、延迟分布） |

---

## 12. 可观测性设计

### 12.1 Trace 链路

每次请求生成 `trace_id`，贯穿全链路：

```
Web UI → AI Gateway → Intent Router → Planner → Tool Executor → LLM → Tool → DB
  └──────────────────────────────── trace_id ───────────────────────────────────┘
```

使用 OpenTelemetry 标准注入和传播 Trace 上下文。

### 12.2 关键指标

| 指标 | 说明 |
|---|---|
| Intent 分类准确率 | 意图识别正确的比例 |
| Plan 执行成功率 | 任务规划成功完成的比例 |
| Tool 调用成功率 | 各工具调用成功/失败比例 |
| 平均延迟 | 从用户输入到最终回答的端到端延迟 |
| SQL 自动修复率 | SQL 失败后自动修复成功的比例 |
| Token 消耗 | 每次会话的 Token 使用量 |
| 幻觉率 | 无数据支撑的结论比例 |
| 用户满意度 | 用户反馈评分 |

### 12.3 Prometheus 指标

| 指标名 | 说明 |
|---|---|
| `llm_latency_seconds` | LLM 调用耗时 |
| `tool_latency_seconds` | Tool 执行耗时（按 tool_name 区分） |
| `token_usage_total` | Token 累计消耗（按 model 区分） |
| `sql_success_rate` | SQL 执行成功率 |
| `tool_call_count_total` | Tool 调用累计次数 |
| `hallucination_rate` | 幻觉率 |
| `circuit_breaker_open` | 熔断器打开数量 |

### 12.4 日志规范

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
  "status": "success",
  "step_id": "s1"
}
```

---

## 13. 技术栈

| 层级 | 推荐技术 | 说明 |
|---|---|---|
| 前端 | React + NextJS | Web UI |
| 后端 | FastAPI (Python) / SpringBoot (Java) | RESTful API |
| Agent 框架 | LangGraph | 状态机 + ReAct 循环 |
| LLM | Claude / GPT-4o / DeepSeek-V3 | 按场景路由 |
| 意图分类 | BERT / BGE-M3 Embedding | 轻量分类模型 |
| 向量库 | Milvus | 长期记忆、Embedding 缓存 |
| OLAP | ClickHouse / Presto | 数据查询 |
| 图数据库 | Neo4j | 元数据血缘存储 |
| 数据库 | MySQL / PostgreSQL | 会话和日志存储 |
| 缓存 | Redis | 上下文缓存、会话状态、限流 |
| 消息队列 | Kafka | 异步任务、事件流 |
| 流处理 | Flink | 实时数据处理 |
| 监控 | Prometheus + Grafana | 指标采集与可视化 |
| Trace | OpenTelemetry | 全链路追踪 |

---

## 14. 风险与应对

| 风险 | 影响 | 应对措施 |
|---|---|---|
| LLM 生成不安全 SQL | 数据泄露 | 多层校验：AST 解析 + 关键字拦截 + 只读账号 + 表白名单 |
| Tool 调用超时 | 用户体验差 | 超时自动降级（缩小查询范围） |
| 上下文丢失 | 多轮对话质量下降 | 关键结果摘要持久化 + Redis 缓存 |
| 意图分类错误 | 执行路径错误 | 低置信度时 LLM 兜底 + 用户确认 |
| 幻觉回答 | 数据不准确 | 强制工具调用 + 结果与结论一致性校验 + JSON Schema 约束输出 |
| Tool 调用风暴 | 系统过载、成本失控 | 次数限制 + 深度限制 + 速率限制 + 熔断机制 |
| Token 成本失控 | 运营成本高 | 模型路由 + Prompt 压缩 + 缓存 + 摘要记忆 |
| Prompt Injection | 安全风险 | 系统 Prompt 隔离 + Tool 白名单 + SQL AST 分析 + 人工确认 |

---

## 15. 实施路线图

### Phase 1：MVP（最小可用）

- AI Gateway + Chat API（鉴权、限流、Trace）
- 实现 `run_sql` 单一工具 + SQL 安全校验（只读 + LIMIT + AST 解析）
- 基础 Intent 识别（规则匹配 + Embedding 分类）
- 简单 ReAct 循环（固定 3 轮）
- Redis 短期记忆
- 基础监控（Prometheus 指标）

### Phase 2：能力扩展

- 接入 `query_metadata`、`kafka`、`flink` 工具
- LLM 驱动的 Planner（LangGraph）
- 多轮上下文管理 + 摘要记忆
- SQL 自动修复
- 模型路由（2-3 个模型）
- 缓存层（Query Cache + Retrieval Cache）

### Phase 3：生产就绪

- 完整权限系统（RBAC + Tool ACL）
- 监控与告警（Prometheus + Grafana + AlertManager）
- 会话持久化与恢复
- Tool 风暴防护（熔断器、速率限制）
- Token 成本优化（Prompt 压缩、LLM Cache）
- 性能优化与压测
- 长期记忆（向量库）

### Phase 4：智能化

- 多 Agent 协同
- AI 根因分析（Root Cause Analysis）
- 自动告警分析
- 自动报告生成
- 用户画像与个性化推荐
