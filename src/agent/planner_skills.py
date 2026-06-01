import re
from dataclasses import dataclass

from src.agent.sql_utils import extract_user_sql


@dataclass(frozen=True)
class PlannerSkill:
    id: str
    name: str
    purpose: str
    instructions: str


_METRIC_RE = re.compile(
    r"(gmv|gm|营收|销售额|利润|毛利|订单|用户|活跃|转化|留存|kpi|指标|pv|uv|复购|新客|客单价)",
    re.IGNORECASE,
)
_ROOT_CAUSE_RE = re.compile(r"(为什么|原因|导致|下滑|下降|下跌|骤降|影响最大|归因)", re.IGNORECASE)
_TREND_RE = re.compile(r"(趋势|波动|近\s*\d+|最近|同比|环比|对比|变化|走势|有没有异常)", re.IGNORECASE)
_PIPELINE_RE = re.compile(
    r"(pipeline|数据链路|数据同步|kafka|flink|checkpoint|反压|积压|消费|topic|消费组|日志|log|告警|alert|报错|宕机|任务状态)",
    re.IGNORECASE,
)
_TROUBLESHOOT_VERB_RE = re.compile(r"(排查|诊断|检查|看看|分析|为什么|怎么了|状态|延迟|集中在哪些|主要集中)", re.IGNORECASE)
_METADATA_RE = re.compile(
    r"(有哪些表|有什么表|哪些表|表结构|表字段|有哪些字段|什么字段|字段有哪些|表信息|表说明|metadata|schema|血缘|lineage|权限|permission|grant|变更历史)",
    re.IGNORECASE,
)
_SQL_ERROR_RE = re.compile(r"(报错|错误|异常|失败|跑不通|有问题|执行不了|语法错误|syntax error|error|检查|看看)", re.IGNORECASE)
_SQL_OPTIMIZATION_RE = re.compile(r"(慢|优化|性能|调优|加速|提速|效率|卡顿|慢查询|执行慢)", re.IGNORECASE)


SKILLS: dict[str, PlannerSkill] = {
    "sql_diagnosis": PlannerSkill(
        id="sql_diagnosis",
        name="SQL 报错诊断",
        purpose="分析用户提供 SQL 的实际执行错误，并基于真实 schema 指出问题。",
        instructions="""\
- 用户提供 SQL 并要求检查、报错分析或定位问题时使用。
- 第一动作必须是用 run_sql 原封不动执行用户提供的 SQL。
- 如果 run_sql 返回字段不存在、表不存在或语法错误，再调用 query_metadata 查看相关表的真实 schema。
- 最终回答必须引用实际错误信息和真实 schema，不要在未执行 SQL 前猜测问题。""",
    ),
    "sql_optimization": PlannerSkill(
        id="sql_optimization",
        name="慢 SQL 优化",
        purpose="基于真实 SQL、表结构和执行反馈给出优化建议。",
        instructions="""\
- 用户询问 SQL 慢、性能、调优、优化建议时使用。
- 优先执行用户 SQL 或检查相关表 schema，识别 JOIN、过滤、排序、聚合和日期范围问题。
- 建议必须落到可执行改法，例如减少扫描范围、补充过滤条件、调整 JOIN 顺序或避免无界 ORDER BY。
- 不要给出脱离当前表结构的通用空话。""",
    ),
    "pipeline_diagnosis": PlannerSkill(
        id="pipeline_diagnosis",
        name="Pipeline 全链路排障",
        purpose="排查 Flink、Kafka、日志和告警相关的数据链路问题。",
        instructions="""\
- 用户提到 pipeline、Kafka、Flink、checkpoint、反压、积压、日志、告警或数据链路异常时使用。
- 优先调用 pipeline_full_diagnosis，一次完成 Flink → Kafka → logs → alerts 的级联检查。
- 如果问题聚焦单一环节，再使用 pipeline_troubleshoot 做 targeted check。
- 最终回答按“结论、证据、影响范围、建议动作”组织。""",
    ),
    "metadata": PlannerSkill(
        id="metadata",
        name="元数据查询",
        purpose="查询表列表、表结构、字段、DDL、血缘或权限信息。",
        instructions="""\
- 用户询问有哪些表、表结构、字段、DDL、血缘、权限、schema 变更时使用。
- 必须调用 query_metadata；不要直接查询 system 表，也不要仅凭 prompt 中的 schema 回答。
- 如果用户问权限、血缘或 schema 变更，优先基于可用业务表生成只读查询，必要时先查表结构。""",
    ),
    "root_cause_analysis": PlannerSkill(
        id="root_cause_analysis",
        name="指标归因分析",
        purpose="解释 GMV、营收、订单等指标下降或异常的原因。",
        instructions="""\
- 用户问“为什么下降/下滑/异常/原因/哪个维度影响最大”时使用。
- 优先调用 root_cause_analysis，并传入明确的 metric。
- 如果用户指定日期、区域或品类，在工具输入或后续查询中保留这些约束。
- 最终回答必须区分整体变化、主要区域/品类、订单交叉验证和可能根因。""",
    ),
    "metric_trend": PlannerSkill(
        id="metric_trend",
        name="指标趋势分析",
        purpose="查询并解释指标趋势、同比环比、异常波动或区域对比。",
        instructions="""\
- 用户问趋势、波动、近 N 天、同比、环比、对比时使用。
- 使用 run_sql 查询实际数据，不要直接调用 root_cause_analysis。
- 查询 metrics 表趋势时必须按日期范围过滤，不要用 LIMIT 伪造最近 N 天。
- metrics 表按 (metric_date, metric_name, region, category) 存储；整体趋势需要过滤 category = '合计' 或按日期聚合。
- 趋势 SQL 应按 metric_date 升序返回，便于解释走势。""",
    ),
    "data_query": PlannerSkill(
        id="data_query",
        name="通用数据查询",
        purpose="查询业务数据、统计结果、排行、聚合和明细。",
        instructions="""\
- 用户要求查数据、统计、列出结果、排行或计算业务指标时使用。
- 如果表结构不确定，先调用 query_metadata；确认后用 run_sql 查询。
- SQL 必须基于已知表和字段，默认限制结果规模。
- 最终回答先给核心结论，再给关键数据明细。""",
    ),
    "direct_answer": PlannerSkill(
        id="direct_answer",
        name="直接回答",
        purpose="无需工具的概念解释、SQL 编写建议或普通对话。",
        instructions="""\
- 只有当问题不需要真实数据、元数据或线上状态时使用。
- 如果进入 planner 后仍发现需要真实数据，改用更具体的工具型 skill。
- 不要声称已经执行查询。""",
    ),
}


def select_planner_skill(user_query: str) -> PlannerSkill:
    """Choose the most specific reusable planner skill for the query."""
    sql = extract_user_sql(user_query)
    if sql and _SQL_OPTIMIZATION_RE.search(user_query):
        return SKILLS["sql_optimization"]
    if sql and _SQL_ERROR_RE.search(user_query):
        return SKILLS["sql_diagnosis"]
    if _PIPELINE_RE.search(user_query) and _TROUBLESHOOT_VERB_RE.search(user_query):
        return SKILLS["pipeline_diagnosis"]
    if _METADATA_RE.search(user_query):
        return SKILLS["metadata"]
    if _METRIC_RE.search(user_query) and _ROOT_CAUSE_RE.search(user_query):
        return SKILLS["root_cause_analysis"]
    if _METRIC_RE.search(user_query) and _TREND_RE.search(user_query):
        return SKILLS["metric_trend"]
    if re.search(r"(查|查询|查下|统计|列出|获取|多少|排名|占比|总数|情况|状态)", user_query, re.IGNORECASE):
        return SKILLS["data_query"]
    return SKILLS["direct_answer"]
