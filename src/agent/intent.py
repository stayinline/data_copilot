import asyncio
import math
import re
from functools import lru_cache

import httpx

from config import EMBEDDING_MODEL, MODEL_API_KEY, MODEL_BASE_URL, SUMMARY_MODEL
from src.agent.llm_client import embed
from src.utils.logging import get_logger

_log = get_logger("intent")

# Greeting / chit-chat -- direct reply, no tool use
_GREETING_RE = re.compile(
    r"^(你好|您好|嗨|hello|hi|hey|早上好|晚上好|中午好|你好啊|"
    r"hi there|hello there|谢谢|感谢|明白了|好的|ok|bye|再见)",
    re.IGNORECASE,
)

# SQL error / troubleshooting -- must mention SQL/query AND an error/problem together
# Two directions: SQL→error (with flexible gap) OR error→SQL (e.g. "报错帮我看看：SELECT ...")
_SQL_TROUBLESHOOT_RE = re.compile(
    r"(sql|SQL|语句|这条sql|查询|这条sql语句).{0,30}"
    r"(报错|错误|异常|失败|跑不通|有问题|出问题了|不行|不通过|执行不了|语法错误|"
    r"sql报错|sql 报错|sql异常|syntax error|error)|"
    r"(报错|错误|异常|失败|语法错误|syntax error).{0,30}"
    r"(sql|SQL|语句|select|SELECT|from|FROM|这条sql)",
    re.IGNORECASE | re.DOTALL,
)

# Pipeline troubleshooting -- diagnose pipeline/kafka/flink issues with action verbs
# Match both directions: verb→keyword OR keyword→verb
_PIPELINE_TROUBLESHOOT_RE = re.compile(
    r"(排查|诊断|检查|查看|看看|帮我|帮我查|为什么|怎么了|出什么|啥问题|为啥).{0,15}"
    r"(kafka|Kafka|flink|Flink|pipeline|Pipeline|数据链路|数据同步|消费|反压|checkpoint|checkpoint_delay|"
    r"积压|延迟|卡顿|堵塞|报错|失败|报错日志|告警|异常|不消费|不工作了|挂了|宕机)|"
    r"(kafka|Kafka|flink|Flink|pipeline|Pipeline|数据链路|消费|反压|checkpoint).{0,20}"
    r"(排查|诊断|检查|分析|看看|为什么|怎么了|啥问题|异常|报错|失败|挂了|宕机|不工作了|不消费|下钻|排查下)",
    re.IGNORECASE,
)

# Metadata query -- asking about available tables, schema, structure, lineage, permissions
# Match BEFORE the general _TOOL_RE to avoid being caught as data query
_METADATA_RE = re.compile(
    r"(有哪些表|有什么表|哪些表|表有哪些|表结构|有哪些表|表列表|metadata|"
    r"可用表|哪些表可用|表名|什么表|表信息|表说明|表字段|"
    r"数据来源|数据血缘|血缘追踪|上游依赖|下游影响|血缘关系|lineage|"
    r"权限|permission|grant|schema变更|变更历史|schema change)",
    re.IGNORECASE,
)

# Data query -- asks for actual data / metrics
_TOOL_RE = re.compile(
    r"(查|查询|查下|查一下|查一查|统计|列出|拉取|获取|跑一下|执行).{0,15}"
    r"(数据|结果|gm|订单|用户|活跃|营收|销售额|利润|毛利|复购|转化|留存|pv|uv|kpi|指标|"
    r"今天|昨天|上周|本月|近7|近30|最近|今年|20\d\d|行数|多少行|大小|占用|"
    r"告警|alert|pipeline|flink|kafka|log|日志|lag|topic|消费组)|"
    r"(多少|几次|频次|频率|总数|总共有|多大).{0,15}"
    r"(告警|订单|用户|活跃|营收|gm|pv|uv|kpi|指标|pipeline|flink|kafka|log|日志|留存|转化|新客|lag|topic|消费组)|"
    r"(gm|gmv|营收|销售额|利润|毛利|订单|用户|活跃|转化|留存|kpi|指标|pv|uv|新客|客单价|flink|kafka|告警|pipeline|lag|topic|消费组|积压).{0,15}"
    r"(是多少|有多少|是多少|数量|情况|状态|趋势|排名|占比|分别是|各是|多大|多少|有多大)",
    re.IGNORECASE,
)

# SQL generation request without execution (includes DDL/create table)
_SQL_WRITE_RE = re.compile(
    r"(帮我?写|生成|帮我想|帮我?创建|建一张|建一个|建表).{0,10}"
    r"(sql|SQL|查询语句|语句|建表语句|create|表|CREATE)",
    re.IGNORECASE,
)

# SQL optimization / performance review -- needs TOOL execution (schema, EXPLAIN, index analysis)
_SQL_OPTIMIZATION_RE = re.compile(
    r"(sql|SQL|语句|查询).{0,25}(慢|优化|性能|调优|加速|提速|效率|卡顿|优化建议|"
    r"怎么优化|如何优化|优化一下|执行慢|很慢|太慢|慢查询|sql优化|sql调优)|"
    r"(优化|调优|优化一下|提速|加速).{0,25}(sql|SQL|语句|查询|性能|慢)",
    re.IGNORECASE | re.DOTALL,
)

# Metric anomaly / diagnostic -- metric name + why changed/anomaly → needs data to answer
_METRIC_DIAG_RE = re.compile(
    r"(gm|gmv|营收|销售额|利润|毛利|订单|用户|活跃|转化|留存|kpi|指标|da|pv|uv|复购|新客|客单价).{0,15}"
    r"(为什么|原因|怎么|怎么下降|为什么下降|为什么上升|为什么涨|为什么跌|异常|波动|下降|上升|涨|跌|低|高|减少|增加|骤降|暴涨)",
    re.IGNORECASE,
)

_INTENT_PROMPT = """\
判断以下用户查询的意图类型，仅返回以下三种之一：
- TOOL: 需要调用工具/执行操作才能回答（如查数据、执行SQL、查元数据、查日志、调API等）
- DIRECT: 可以直接用已有知识回答（如概念解释、SQL编写建议、SQL优化、知识问答、闲聊等）
- TROUBLESHOOT: 需要排查线上问题、分析异常（可能涉及多个工具调用）

注意：
- "帮我写一条SQL" → DIRECT（只需编写，不需要执行）
- "这条SQL很慢怎么优化" → TOOL（需要查实际表结构、索引、执行计划EXPLAIN，基于真实数据给优化建议）
- "这条SQL报错帮我看看什么问题" → TROUBLESHOOT（需要分析具体SQL的错误原因）
- "查一下昨天的GMV" → TOOL（需要实际执行SQL获取数据）
- "有哪些表可用" → TOOL（需要调用 query_metadata 工具查看元数据）
- "kafka消费慢是什么原因" → DIRECT（通用知识问答）
- "帮我排查一下kafka积压" → TROUBLESHOOT（需要实际查看积压数据）

用户查询：{query}
返回意图类型（仅返回标签名称）："""

# ── Embedding-based Layer 2 classification ──

# Pre-defined intent examples covering each intent type
_INTENT_EXAMPLES = [
    # TOOL examples — data queries requiring actual tool execution
    ("TOOL", "昨天各区域的销售额是多少"),
    ("TOOL", "这个月的新用户注册量"),
    ("TOOL", "近7天GMV趋势"),
    ("TOOL", "统计上个月订单转化率"),
    ("TOOL", "查询活跃用户数"),
    ("TOOL", "看一下今天的营收数据"),
    ("TOOL", "有哪些表可以用"),
    ("TOOL", "orders表的结构是什么"),
    ("TOOL", "查看dws_order表的字段"),
    ("TOOL", "Pipeline状态怎么样"),
    ("TOOL", "最近有告警吗"),
    ("TOOL", "kafka消费积压情况"),
    ("TOOL", "flink任务状态"),
    ("TOOL", "今天GMV为什么下降了"),
    ("TOOL", "这周一共触发了多少次告警"),
    ("TOOL", "上个月各区域销售额对比"),
    ("TOOL", "金卡和黑金会员的购买行为特点"),
    ("TOOL", "dws_order_daily这张表的数据来源是什么"),
    ("TOOL", "这张表的血缘关系是什么"),
    ("TOOL", "金卡会员主要来自哪些城市"),
    ("TOOL", "这条SQL执行很慢怎么优化"),
    ("TOOL", "帮我优化一下这个SQL查询"),
    # DIRECT examples — can answer from knowledge without tool execution
    ("DIRECT", "什么是数据仓库"),
    ("DIRECT", "SQL中JOIN和LEFT JOIN的区别"),
    ("DIRECT", "帮我写一个统计GMV的SQL"),
    ("DIRECT", "ClickHouse和MySQL有什么区别"),
    ("DIRECT", "谢谢"),
    ("DIRECT", "你好"),
    ("DIRECT", "kafka消费慢可能是什么原因"),
    ("DIRECT", "Flink反压的原理是什么"),
    ("DIRECT", "我想在ClickHouse里建一张订单日汇总表"),
    ("DIRECT", "帮我写一条SQL统计会员等级"),
    ("DIRECT", "销售日报看板应该包含哪些指标"),
    # TROUBLESHOOT examples — requires diagnosing issues across systems
    ("TROUBLESHOOT", "帮我排查kafka消费积压"),
    ("TROUBLESHOOT", "flink任务报错了怎么办"),
    ("TROUBLESHOOT", "数据链路出问题了"),
    ("TROUBLESHOOT", "帮我检查一下Pipeline"),
    ("TROUBLESHOOT", "为什么数据没有入库"),
    ("TROUBLESHOOT", "kafka消费者不消费了"),
    ("TROUBLESHOOT", "flink checkpoint失败"),
    ("TROUBLESHOOT", "帮我排查一下数据延迟"),
    ("TROUBLESHOOT", "这条SQL报错帮我看看什么问题"),
    ("TROUBLESHOOT", "SELECT语句执行报错，帮我检查"),
    ("TROUBLESHOOT", "数据同步Pipeline从昨晚开始频繁报错"),
]

# Cosine similarity threshold — above this, trust the embedding match
EMBEDDING_CONFIDENCE_THRESHOLD = 0.78

# Cached embeddings and texts
_embedding_vectors: list[list[float]] | None = None
_embedding_texts: list[str] | None = None
_embedding_intents: list[str] | None = None
_embedding_init_done = False
_embedding_init_lock: asyncio.Lock | None = None


def _cosine_similarity(a: list[float], b: list[float]) -> float:
    """Compute cosine similarity between two vectors."""
    dot = sum(x * y for x, y in zip(a, b))
    norm_a = math.sqrt(sum(x * x for x in a))
    norm_b = math.sqrt(sum(x * x for x in b))
    if norm_a == 0 or norm_b == 0:
        return 0.0
    return dot / (norm_a * norm_b)


async def _ensure_embeddings():
    """Lazy-load example embeddings on first use."""
    global _embedding_vectors, _embedding_texts, _embedding_intents, _embedding_init_done, _embedding_init_lock

    if _embedding_init_lock is None:
        _embedding_init_lock = asyncio.Lock()

    async with _embedding_init_lock:
        if _embedding_init_done:
            return

        texts = [text for _, text in _INTENT_EXAMPLES]
        _log.debug("Computing embeddings for %d intent examples", len(texts))
        vectors = await embed(texts, model=EMBEDDING_MODEL)
        _embedding_vectors = vectors
        _embedding_texts = texts
        _embedding_intents = [intent for intent, _ in _INTENT_EXAMPLES]
        _embedding_init_done = True
        _log.debug("Intent embeddings computed, dimension=%d", len(vectors[0]) if vectors else 0)


async def _embedding_classify(query: str) -> tuple[str | None, float]:
    """Layer 2: Embedding similarity matching.

    Returns (intent, confidence) or (None, 0) if below threshold.
    """
    global _embedding_vectors, _embedding_texts, _embedding_intents, _embedding_init_done

    if not _embedding_init_done:
        await _ensure_embeddings()

    if not _embedding_vectors or not _embedding_texts:
        return None, 0.0

    # Compute embedding for the query
    query_vectors = await embed([query], model=EMBEDDING_MODEL)
    if not query_vectors:
        return None, 0.0

    query_vec = query_vectors[0]

    # Find best matching example
    best_score = 0.0
    best_idx = 0
    for i, example_vec in enumerate(_embedding_vectors):
        score = _cosine_similarity(query_vec, example_vec)
        if score > best_score:
            best_score = score
            best_idx = i

    best_intent = _embedding_intents[best_idx]
    best_text = _embedding_texts[best_idx]

    _log.debug("embedding match: best='%s' (%s) similarity=%.4f",
               best_text, best_intent, best_score)

    if best_score >= EMBEDDING_CONFIDENCE_THRESHOLD:
        return best_intent, best_score

    return None, best_score


@lru_cache(maxsize=512)
def _llm_classify(query: str) -> str:
    """Use LLM to classify intent. Results cached."""
    try:
        with httpx.Client(timeout=15) as client:
            resp = client.post(
                f"{MODEL_BASE_URL}/chat/completions",
                headers={"Authorization": f"Bearer {MODEL_API_KEY}"},
                json={
                    "model": SUMMARY_MODEL,
                    "messages": [
                        {"role": "user", "content": _INTENT_PROMPT.format(query=query)}
                    ],
                    "temperature": 0.0,
                },
            )
            resp.raise_for_status()
            result = resp.json()["choices"][0]["message"]["content"].strip().upper()
            if result in ("TOOL", "DIRECT", "TROUBLESHOOT"):
                return result
            return "DIRECT"
    except Exception as e:
        _log.debug("_llm_classify error: %s", str(e))
        return "DIRECT"


class IntentClassifier:
    """Three-tier intent classifier: rules → embedding similarity → LLM fallback.

    Tier 1: Regex rules (fast path, covers common patterns)
    Tier 2: Embedding cosine similarity (covers unseen patterns similar to examples)
    Tier 3: LLM classification (fallback for low-confidence matches)
    """

    @staticmethod
    async def classify_with_confidence(query: str) -> tuple[str, bool]:
        """Classify intent and return (intent_label, is_high_confidence).

        High confidence = matched by regex (Tier 1) or embedding (Tier 2).
        Low confidence = fell through to LLM (Tier 3), user may need guidance.
        """
        if _GREETING_RE.search(query):
            _log.debug("classify=%s (greeting)", "DIRECT")
            return "DIRECT", True

        if _SQL_TROUBLESHOOT_RE.search(query):
            _log.debug("classify=%s (sql troubleshoot)", "TROUBLESHOOT")
            return "TROUBLESHOOT", True

        if _PIPELINE_TROUBLESHOOT_RE.search(query):
            _log.debug("classify=%s (pipeline troubleshoot)", "TROUBLESHOOT")
            return "TROUBLESHOOT", True

        if _METADATA_RE.search(query):
            _log.debug("classify=%s (metadata query)", "TOOL")
            return "TOOL", True

        if _SQL_WRITE_RE.search(query):
            _log.debug("classify=%s (sql write)", "DIRECT")
            return "DIRECT", True

        if _SQL_OPTIMIZATION_RE.search(query):
            _log.debug("classify=%s (sql optimization)", "TOOL")
            return "TOOL", True

        if _METRIC_DIAG_RE.search(query):
            _log.debug("classify=%s (metric diagnostic)", "TOOL")
            return "TOOL", True

        if _TOOL_RE.search(query):
            _log.debug("classify=%s (data query)", "TOOL")
            return "TOOL", True

        # Tier 2: Embedding similarity matching
        _log.debug("classify falling back to embedding similarity")
        intent, confidence = await _embedding_classify(query)
        if intent:
            _log.debug("classify=%s (embedding, confidence=%.4f)", intent, confidence)
            return intent, True

        # Tier 3: LLM classification (cached) — LOW CONFIDENCE
        _log.debug("classify falling back to LLM")
        result = _llm_classify(query)
        _log.debug("classify=%s (LLM)", result)
        return result, False

    @staticmethod
    async def classify(query: str) -> str:
        intent, _ = await IntentClassifier.classify_with_confidence(query)
        return intent

    # Suggested example questions shown when confidence is low
    GUIDANCE_EXAMPLES = [
        ("📊 查数据", "昨天的GMV总量是多少"),
        ("📈 看趋势", "近7天的GMV趋势怎么样，有没有异常波动"),
        ("🔍 找原因", "今天GMV为什么下降了"),
        ("🛠 写SQL", "帮我写一条SQL，统计每个会员等级的购买金额"),
        ("🔧 查表结构", "orders表有哪些字段"),
        ("🚨 排查故障", "Kafka消费延迟很高，全面诊断一下整个链路"),
    ]
