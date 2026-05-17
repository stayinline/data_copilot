import json
import re

from langgraph.graph import StateGraph, END

from src.agent.planner_state import PlannerState
from src.agent.llm_client import chat_completion
from src.agent.sql_fix import auto_fix_sql
from src.tools.base import ToolRegistry
from src.sql.validator import SqlValidator
from src.sql.schema_loader import SCHEMA_TEXT, ALLOWED_TABLES
from src.utils.logging import get_logger
from config import (
    CHAT_MODEL,
    PLANNER_MAX_DEPTH,
    PLANNER_MAX_TOOL_CALLS,
)

_log = get_logger("planner.graph")

from src.tools.base import ToolRegistry


def _extract_user_sql(query: str) -> str | None:
    """Extract a SQL statement from a user query that asks for SQL diagnosis.

    Matches patterns like:
    - "这条SQL报错帮我看看什么问题：SELECT ..."
    - "帮我看看这个SQL：SELECT ..."
    - Queries containing SELECT/INSERT/UPDATE/DELETE with a clear SQL statement
    """
    import re

    # Match SQL keywords that indicate an embedded SQL statement
    sql_match = re.search(
        r"(SELECT|select|INSERT|insert|UPDATE|update|DELETE|delete|CREATE|create|DROP|drop)\b"
        r"\s+.*",
        query,
        re.DOTALL,
    )
    if not sql_match:
        return None

    extracted = sql_match.group(0).strip()
    # Remove trailing markdown or prompts
    extracted = re.split(r"\s*```", extracted)[0]
    extracted = re.split(r"\s*\n\s*(帮我|请|看看|分析|检查|谢谢)", extracted)[0]
    # If it ends with a SQL keyword, trim it
    extracted = extracted.rstrip(" ,;")
    return extracted if len(extracted) > 10 else None


def _build_planner_prompt(
    user_query: str = "",
    previous_analysis: str = "",
    tool_results: list[dict] | None = None,
) -> str:
    tools_info = _build_tools_info()

    # ── Detect embedded SQL in user query and inject as top instruction ──
    sql_diagnosis_block = ""
    if user_query:
        extracted_sql = _extract_user_sql(user_query)
        if extracted_sql:
            sql_diagnosis_block = (
                f"## SQL 诊断任务（最高优先级）\n\n"
                f"用户在下面提供了一条 SQL，要求你帮他检查问题。\n"
                f"**你必须先原封不动地执行这条 SQL，再根据实际返回的结果（数据或报错信息）来分析问题。**\n"
                f"不要查 schema 就开始空分析，不要写新的 SQL 来代替用户提供的 SQL。\n\n"
                f"用户提供的 SQL：\n```sql\n{extracted_sql}\n```\n\n"
                f"**你的第一步：用 run_sql 工具执行上面的 SQL。这是你必须做的第一件事。**\n"
                f"**如果 SQL 执行报错（如 Unknown identifier / 字段不存在 / 表不存在），下一步必须调用 query_metadata 工具（query_type=\"schema\"）查看该表的实际字段，确认用户的 SQL 中哪些地方写错了，然后基于真实 schema 指出问题所在。**\n"
            )

    prompt = f"""\
你是一个数据平台的规划器。你的任务是基于用户的问题，决定下一步该做什么。
{sql_diagnosis_block}

可用数据表结构（仅供参考，实际表结构以 query_metadata 工具返回为准）：
{SCHEMA_TEXT}

可用工具：
{tools_info}

调用工具格式：{{"name": "工具名", "input": {{...}}}}
给出最终回答格式：{{"name": "final_answer", "content": "回答内容"}}

重要提示（必须遵守）：
- 当用户询问"有哪些表"、"表结构"、"表字段"等元数据问题时，必须调用 query_metadata 工具（query_type="list_tables" 或 "schema"），绝对不要写 SQL 查询 system 表，绝对不要直接用 prompt 中的表结构信息直接回答
- 当用户询问业务数据（如 GMV、订单量等）时，调用 run_sql 工具写 SQL 查询
- **当用户提供了一条 SQL 并要求"帮我看看什么问题"、"这条 SQL 报错"、"帮我检查 SQL"时：先用 run_sql 执行该 SQL，根据实际返回结果（数据或报错信息）来分析问题，不要只查 schema 就开始空分析**
- **query_metadata 返回 schema 后，必须继续用 run_sql 查询实际数据，不要把 schema 本身当作最终答案**
- **区分趋势分析和归因分析**：
  - "趋势怎么样"、"有没有异常波动"、"各区域对比" → 用 run_sql 查数据，然后分析数据给出结论。查询 metrics 表时注意：该表按 (metric_date, metric_name, region, category) 四维度存储，每个日期有多条记录。**查趋势时 SQL 必须同时满足以下三点**：
    1. 过滤 region 和 category：`WHERE region = '华东' AND category = '合计'`（如果用户指定了区域）或 `WHERE category = '合计'`（如果用户查整体）
    2. 用日期范围代替 LIMIT：`WHERE metric_date >= (SELECT max(metric_date) FROM metrics) - INTERVAL 7 DAY`
    3. 按日期排序：`ORDER BY metric_date ASC`
    错误示例：`SELECT metric_date, metric_value FROM metrics WHERE metric_name = 'GMV' ORDER BY metric_date DESC LIMIT 7` — 这只会返回同一天的 7 条不同 region 的数据
    正确示例：`SELECT metric_date, sum(metric_value) as gmv FROM metrics WHERE metric_name = 'GMV' AND metric_date >= (SELECT max(metric_date) FROM metrics) - INTERVAL 7 DAY GROUP BY metric_date ORDER BY metric_date ASC`
  - "为什么下降"、"是什么原因导致下跌"、"哪个区域影响最大" → 调用 root_cause_analysis 工具（metric=指标名）
- **如果已经通过 run_sql 获取了趋势数据，直接分析数据给出结论（final_answer），不要额外调用 root_cause_analysis**
- 当用户询问 Pipeline 排障相关问题（如 Kafka 消费延迟、Flink 任务状态、Pipeline 报错日志、告警记录、数据链路故障等）时，优先调用 pipeline_full_diagnosis 工具进行全链路自动诊断；如果需要针对某个环节深入排查，再调用 pipeline_troubleshoot 工具（operation: check_kafka/check_flink/check_logs/check_alerts）
- SCHEMA_TEXT 仅供参考，所有工具查询操作必须通过 query_metadata 工具，不要跳过工具调用
- **每次只能调用一个工具**，不要同时调用多个工具，需要依次调用

规则：
- 所有结论必须有数据支撑
- 先给出核心结论，再补充数据细节和分析说明
- 适当解读数据背后的含义，如趋势、对比、异常等
- 不要编造数据
- 如果不确定表结构，先调用 query_metadata 工具查看 schema
- 如果问题已经回答，直接输出 final_answer
- SQL 生成必须基于上述表结构，不要虚构表名或字段名

关键行为准则：
- **如果上一步调用的是 query_metadata 且返回了 schema/表结构，下一步必须写 SQL（run_sql）来查询用户实际要的数据，绝对不能把表结构当作最终答案**
- **如果上一步已经执行了工具并返回了数据结果，你必须推进到下一步操作（如调用 run_sql 执行查询），绝对不要重复调用已经成功的工具**
- **如果已经获得了足够回答问题的数据，立即输出 final_answer**
- **不要重复已经执行过的工具调用，每次调用都应该是新的、不同的操作**
- **分析 SQL 报错时，必须先看 run_sql 的实际返回结果（数据或错误信息），不要脱离实际执行结果做猜测**
- **ClickHouse 的 ORDER BY 默认 ASC，不需要强制写 ASC/DESC，这不是语法错误**
- **如果 SQL 返回空结果（"columns": [], "rows": [] 或数据为空），必须如实告知用户没有数据，并分析可能原因（如日期范围不对、过滤条件太严等），绝对不要编造数据或表格**
- **绝对禁止编造任何数据——所有数字、表格、区域名称必须来自实际执行结果，不要 "举例说明"、"假设数据"、"例如"**
"""
    # Inject already-executed tool calls to prevent LLM from repeating them
    if tool_results:
        # Group by tool+input to show repetition count
        from collections import Counter
        call_counts = Counter()
        for tr in tool_results:
            tool = tr.get("tool", "unknown")
            inp = json.dumps(tr.get("input", {}), ensure_ascii=False)
            call_counts[(tool, inp)] += 1

        prompt += "\n\n## 已执行的工具调用（不要重复调用）\n\n"
        seen = set()
        for i, tr in enumerate(tool_results, 1):
            tool = tr.get("tool", "unknown")
            inp = tr.get("input", {})
            output = tr.get("output", "")[:200]
            key = json.dumps({"tool": tool, "input": inp}, ensure_ascii=False)
            if key in seen:
                continue
            seen.add(key)
            count = call_counts.get((tool, json.dumps(inp, ensure_ascii=False)), 1)
            status = "成功" if tr.get("success") else "失败"
            repeat_note = f"（已重复调用 {count} 次！）" if count > 1 else ""
            prompt += f"{i}. [{tool}] input={json.dumps(inp, ensure_ascii=False)} → {status}{repeat_note}\n   返回: {output}\n"
        prompt += "\n**你已经执行过以上工具。如果数据已足够回答用户问题，输出 final_answer；如果需要新操作，必须是不同的工具或不同的输入。**\n"

    if previous_analysis:
        prompt += f"\n\n## 历史分析上下文\n\n以下是之前几轮对话中已执行的分析结果，请基于这些历史结论决定下一步：\n\n{previous_analysis}\n"
    return prompt


def _build_tools_info() -> str:
    """Build a description of available tools from ToolRegistry."""
    tools = ToolRegistry.list_all()
    lines = []
    for t in tools:
        lines.append(f"- {t['name']}: {t['description']}")
        props = t["input_schema"].get("properties", {})
        for key, val in props.items():
            desc = val.get("description", "")
            if val.get("enum"):
                desc += f" (可选值: {', '.join(val['enum'])})"
            lines.append(f"    input.{key}: {desc}")
    lines.append("- final_answer: 给出最终自然语言回答")
    return "\n".join(lines)


async def planner_node(state: PlannerState) -> dict:
    return await _planner_node_impl(state, use_streaming=False)


async def planner_node_stream(state: PlannerState) -> dict:
    """Streaming variant of planner_node: emits tokens via callback."""
    token_callback = state.get("_token_callback")
    return await _planner_node_impl(state, use_streaming=True, token_callback=token_callback)


async def _planner_node_impl(state: PlannerState, use_streaming: bool = False, token_callback=None) -> dict:
    """Planner LLM node: decides next action (tool call or final answer).

    With Annotated[list, operator.add] state reducers, this node returns
    ONLY new items (delta) for list fields, not the full accumulated list.
    """
    _log.debug("planner_node  msg_count=%d", len(state.get("messages", [])))
    previous_analysis = state.get("previous_analysis", "")
    tool_results = state.get("tool_results", [])
    user_query = state.get("user_query", "")
    messages = [{"role": "system", "content": _build_planner_prompt(user_query, previous_analysis, tool_results)}]
    messages.extend(state.get("messages", []))

    usage = {}
    try:
        if use_streaming and token_callback:
            response = await _stream_with_callback(messages, token_callback, usage)
        else:
            response = await chat_completion(
                messages, model=CHAT_MODEL, temperature=0.1, max_retries=1, collect_usage=usage
            )
    except Exception as e:
        _log.error("planner_node LLM error: %s", str(e))
        return {
            "final_answer": "抱歉，查询超时，请稍后重试。",
            "messages": [{"role": "assistant", "content": "抱歉，查询超时，请稍后重试。"}],
            "_usage_list": state.get("_usage_list", []) + ([usage] if usage else []),
        }
    _log.debug("planner response_len=%d", len(response))

    # Try to parse as tool call
    try:
        tool_call = _extract_tool_call(response)
        if tool_call:
            # "final_answer" means done — set final_answer directly
            if tool_call.get("name") == "final_answer":
                return {
                    "final_answer": tool_call.get("content", response),
                    "messages": [{"role": "assistant", "content": response}],
                    "_usage_list": state.get("_usage_list", []) + ([usage] if usage else []),
                }
            return {
                "messages": [{"role": "assistant", "content": response}],
                "tool_call_count": state["tool_call_count"] + 1,
                "_next_action": tool_call,
                "_usage_list": state.get("_usage_list", []) + ([usage] if usage else []),
            }
    except Exception:
        pass

    # No valid tool call -> treat as final answer
    return {
        "final_answer": response,
        "messages": [{"role": "assistant", "content": response}],
        "_usage_list": state.get("_usage_list", []) + ([usage] if usage else []),
    }


async def _stream_with_callback(messages: list[dict], callback, usage: dict | None = None) -> str:
    """Stream LLM response, calling callback(token) for each token. Returns full text."""
    import time
    from src.agent.llm_client import chat_completion_stream, CHAT_MODEL
    start = time.monotonic()
    full = []
    async for token in chat_completion_stream(messages, model=CHAT_MODEL, temperature=0.1):
        full.append(token)
        callback(token)
    if usage is not None:
        usage["latency_ms"] = round((time.monotonic() - start) * 1000, 1)
        usage["llm_calls"] = 1
    return "".join(full)


async def executor_node(state: PlannerState) -> dict:
    """Executor node: parse tool call -> execute -> collect observation."""
    messages = state.get("messages", [])
    last_msg = messages[-1]["content"] if messages else ""

    tool_call = _extract_tool_call(last_msg)
    if not tool_call:
        return {"observations": state.get("observations", []) + ["No tool call to execute"]}

    tool_name = tool_call.get("name", "run_sql")
    tool_input = tool_call.get("input", tool_call)
    if "name" in tool_input:
        tool_input = {k: v for k, v in tool_input.items() if k != "name"}

    _log.debug("executor_node  tool=%s", tool_name)

    observation = None
    tool = ToolRegistry.get(tool_name)

    # Track tool performance
    tool_latency_ms = 0.0
    tool_from_cache = False

    if not tool:
        observation = f"Unknown tool: {tool_name}"

    elif tool_name == "run_sql":
        query = tool_input.get("query", "")

        # ── Pre-execution dedup guard: refuse to re-run identical SQL ──
        # This is a safety net in case _should_continue dedup fails.
        def _norm(s: str) -> str:
            s = s.strip().rstrip(";").strip()
            s = re.sub(r"\s+LIMIT\s+\d+\s*$", "", s, flags=re.IGNORECASE).strip()
            return s.upper()

        proposed_norm = _norm(query)
        for tr in state.get("tool_results", []):
            if tr.get("tool") != "run_sql":
                continue
            stored_norm = _norm((tr.get("input") or {}).get("query", ""))
            if proposed_norm == stored_norm or proposed_norm.startswith(stored_norm) or stored_norm.startswith(proposed_norm):
                _log.warning("executor dedup guard: blocking repeated run_sql call")
                observation = "检测到重复执行相同的 SQL 查询，已跳过。请基于已有结果总结回答。"
                # Still return a step_result but mark as skipped
                step_result = {
                    "tool": tool_name,
                    "input": tool_input,
                    "output": observation,
                    "success": True,
                    "latency_ms": 0,
                    "from_cache": False,
                    "skipped_by_dedup": True,
                }
                return {
                    "observations": [observation],
                    "tool_results": [step_result],
                    "messages": [{"role": "system", "content": f"Observation: {observation}"}],
                }

        validation = SqlValidator.validate(query)
        if not validation.success:
            observation = f"SQL validation failed: {'; '.join(validation.errors)}"
        tool_input["query"] = validation.sanitized_sql or query

        # ── Detect if this is a user SQL diagnosis request ──
        # When user provides their own SQL and asks "帮我看看什么问题",
        # we should NOT auto-fix — the user wants the error explained, not silently corrected.
        _user_query = state.get("user_query", "")
        _is_user_sql_diagnosis = False
        if _user_query:
            _extracted = _extract_user_sql(_user_query)
            if _extracted:
                # Compare normalized forms to see if this is the user's SQL
                _user_norm = _norm(_extracted)
                _proposed_norm = _norm(query)
                if _user_norm == _proposed_norm or _user_norm.startswith(_proposed_norm) or _proposed_norm.startswith(_user_norm):
                    _is_user_sql_diagnosis = True

        if observation is None:
            result = await tool.execute(tool_input)
            tool_latency_ms = result.latency_ms
            tool_from_cache = result.from_cache
            if result.success:
                observation = json.dumps(result.data, ensure_ascii=False)
            elif _is_user_sql_diagnosis:
                # User asked to diagnose their SQL — return error AND proactively check actual schema
                # so the planner has real data (not just error text) to analyze.
                _meta_tool = ToolRegistry.get("query_metadata")
                _schema_info = ""
                if _meta_tool:
                    # Extract table name from the SQL for targeted schema lookup
                    _matched_table = None
                    for _tbl in sorted(
                        [t for t in ALLOWED_TABLES if re.search(rf"\b{t}\b", query, re.IGNORECASE)],
                        key=len, reverse=True
                    ):
                        _matched_table = _tbl
                        break
                    if _matched_table:
                        try:
                            _meta_result = await _meta_tool.execute({"query_type": "schema", "table_name": _matched_table})
                            if _meta_result.success:
                                _data = _meta_result.data
                                _cols = ", ".join(c["name"] for c in _data.get("columns", []))
                                _schema_info = f"\n【实际表结构】{_matched_table} 的字段：{_cols}"
                        except Exception as _e:
                            _log.debug("query_metadata during SQL diagnosis failed: %s", _e)
                observation = f"【SQL 执行错误】该 SQL 执行失败，错误信息：{result.error}{_schema_info or ''}"
            else:
                # SQL auto-fix (for LLM-generated SQL in normal queries)
                fixed_sql, fix_status = await auto_fix_sql(
                    original_sql=tool_input.get("query", ""),
                    error_message=result.error,
                )
                if fixed_sql:
                    result2 = await tool.execute({"query": fixed_sql})
                    if result2.success:
                        original_sql = tool_input.get("query", "")
                        observation = (
                            f"【SQL 自动修复】原始 SQL 执行失败（错误：{result.error}），"
                            f"已自动修正为：{fixed_sql}。以下查询结果基于修正后的 SQL：\n"
                            + json.dumps(result2.data, ensure_ascii=False)
                        )
                        tool_input["query"] = fixed_sql
                        state["tool_results"].append({
                            "tool": "sql_auto_fix",
                            "input": {"original_sql": original_sql, "fixed_sql": fixed_sql, "original_error": result.error},
                            "output": f"原始 SQL 失败（{result.error}），已修正为：{fixed_sql}",
                            "success": True,
                            "is_sql_fix": True,
                            "latency_ms": round(result2.latency_ms, 1),
                            "from_cache": result2.from_cache,
                        })
                    else:
                        observation = f"【SQL 自动修复失败】原始错误：{result.error}。修复后的 SQL 仍然失败：{result2.error}"
                else:
                    # Auto-fix couldn't generate a fix
                    observation = f"Error: {result.error}"

    else:
        # Generic tool execution (e.g., query_metadata)
        result = await tool.execute(tool_input)
        tool_latency_ms = result.latency_ms
        tool_from_cache = result.from_cache
        if result.success:
            observation = json.dumps(result.data, ensure_ascii=False)
        else:
            observation = f"Error: {result.error}"

    step_result = {
        "tool": tool_name,
        "input": tool_input,
        "output": observation,
        "success": not (
            observation.startswith("Error:") or "validation failed" in observation
        ),
        "latency_ms": round(tool_latency_ms, 1),
        "from_cache": tool_from_cache,
    }

    # With Annotated[list, operator.add], return ONLY the delta (new items).
    # LangGraph accumulates these via add, so returning the full list would duplicate.
    return {
        "observations": [observation],
        "tool_results": [step_result],
        "messages": [{"role": "system", "content": f"Observation: {observation}"}],
    }


def _should_continue(state: PlannerState) -> str:
    """Conditional edge: check if we should continue or finish."""
    # Storm protection
    if state.get("tool_call_count", 0) >= PLANNER_MAX_TOOL_CALLS:
        return "summarizer"

    # If final_answer already set, finish
    if state.get("final_answer"):
        return "summarizer"

    # Check if planner produced a tool call
    messages = state.get("messages", [])
    if messages:
        last_msg = messages[-1]["content"]
        tool_call = _extract_tool_call(last_msg)
        if tool_call:
            tool_results = state.get("tool_results", [])
            tool_name = tool_call.get("name", "")
            tool_input = tool_call.get("input", {})
            if "name" in tool_input:
                tool_input = {k: v for k, v in tool_input.items() if k != "name"}

            if tool_name == "run_sql":
                # For SQL dedup: normalize by stripping trailing LIMIT clauses
                # (validator adds LIMIT 1000, so proposed SQL is prefix of stored SQL)
                def _normalize_sql(s: str) -> str:
                    s = s.strip().rstrip(";").strip()
                    # Remove trailing LIMIT clause for comparison
                    s = re.sub(r"\s+LIMIT\s+\d+\s*$", "", s, flags=re.IGNORECASE).strip()
                    return s.upper()

                proposed_sql = _normalize_sql(tool_input.get("query", ""))
                for tr in tool_results:
                    if tr.get("tool") != "run_sql":
                        continue
                    stored_sql = _normalize_sql((tr.get("input") or {}).get("query", ""))
                    # Match if proposed SQL is same or is a prefix of stored (or vice versa)
                    if proposed_sql == stored_sql or proposed_sql.startswith(stored_sql) or stored_sql.startswith(proposed_sql):
                        _log.info("dedup detected: repeated run_sql call, forcing summarize")
                        return "summarizer"
            else:
                # Generic dedup for non-SQL tools
                tool_input_normalized = frozenset({k: str(v) for k, v in tool_input.items()}.items())
                same_calls = sum(
                    1 for tr in tool_results
                    if tr.get("tool") == tool_name
                    and frozenset({k: str(v) for k, v in (tr.get("input") or {}).items()}.items()) == tool_input_normalized
                )
                # Allow up to 2 identical calls before forcing summarize.
                # This gives the planner room to retry if the first call failed or was incomplete,
                # but prevents infinite loops.
                if same_calls >= 2:
                    _log.info("dedup detected: %s called %d times with same input, forcing summarize",
                              tool_name, same_calls + 1)
                    return "summarizer"
            return "executor"

    return "summarizer"


def _check_max_depth(state: PlannerState) -> str:
    """Check if we've exceeded max depth."""
    if len(state.get("observations", [])) >= PLANNER_MAX_DEPTH:
        return "summarizer"
    return "continue"


async def summarizer_node(state: PlannerState) -> dict:
    return await _summarizer_node_impl(state, use_streaming=False)


async def summarizer_node_stream(state: PlannerState) -> dict:
    """Streaming variant of summarizer_node: emits tokens via callback."""
    token_callback = state.get("_token_callback")
    return await _summarizer_node_impl(state, use_streaming=True, token_callback=token_callback)


async def _summarizer_node_impl(state: PlannerState, use_streaming: bool = False, token_callback=None) -> dict:
    """Summarizer node: generate final answer if not already set."""
    if state.get("final_answer"):
        return {}

    # Build summary prompt with explicit anti-hallucination rules
    tool_results = state.get("tool_results", [])
    observations = state.get("observations", [])

    # Check if all SQL results are empty
    all_sql_empty = all(
        '"columns": []' in tr.get("output", "") and '"rows": []' in tr.get("output", "")
        for tr in tool_results if tr.get("tool") == "run_sql"
    ) if tool_results else False

    summary_rules = "基于以上观察结果，总结并回答用户问题。"
    if all_sql_empty:
        summary_rules += "\n\n**重要：SQL 查询返回了空结果（无数据）。你必须如实告知用户没有数据，并分析可能原因（如日期范围不在数据覆盖范围内、过滤条件太严格等）。绝对不要编造任何数据、表格或数字！**"

    summary_rules += "\n\n规则：\n- 所有数字和表格必须来自实际执行结果\n- 绝对禁止编造任何数据、示例数字或假设性表格\n- 如果没有数据，就说明情况，不要假装查到了数据"

    messages = [{"role": "system", "content": summary_rules}]
    messages.extend(state.get("messages", []))

    usage = {}
    try:
        if use_streaming and token_callback:
            response = await _stream_with_callback(messages, token_callback, usage)
        else:
            response = await chat_completion(messages, model=CHAT_MODEL, temperature=0.1, max_retries=1, collect_usage=usage)
    except Exception as e:
        _log.error("summarizer_node LLM error: %s", str(e))
        obs = state.get("observations", [])
        answer = "查询超时，未能获取结果。"
        if obs:
            answer = "抱歉，部分查询超时。以下是已获取的结果：\n" + "\n".join(str(o) for o in obs[:5])
        return {"final_answer": answer, "messages": [{"role": "assistant", "content": answer}],
                "_usage_list": state.get("_usage_list", []) + ([usage] if usage else [])}
    return {"final_answer": response, "messages": messages + [{"role": "assistant", "content": response}],
            "_usage_list": state.get("_usage_list", []) + ([usage] if usage else [])}


def _extract_tool_call(response: str) -> dict | None:
    """Extract a tool call JSON from the LLM response."""
    start = response.find("{")
    if start == -1:
        return None
    depth = 0
    for i, ch in enumerate(response[start:], start):
        if ch == "{":
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0:
                candidate = response[start:i + 1]
                if '"name"' in candidate:
                    try:
                        return json.loads(candidate)
                    except json.JSONDecodeError:
                        pass
                break
    return None


def build_planner_graph():
    """Build and compile the LangGraph planner StateGraph."""
    graph = StateGraph(PlannerState)

    # Add nodes
    graph.add_node("planner", planner_node)
    graph.add_node("executor", executor_node)
    graph.add_node("summarizer", summarizer_node)

    # Entry point
    graph.set_entry_point("planner")

    # Conditional edge from planner
    graph.add_conditional_edges("planner", _should_continue, {"executor": "executor", "summarizer": "summarizer"})

    # Back to planner after executor
    graph.add_edge("executor", "planner")

    # Summarizer is terminal
    graph.add_edge("summarizer", END)

    return graph.compile()


def build_planner_graph_stream():
    """Build a streaming-capable planner graph with streaming-aware nodes."""
    graph = StateGraph(PlannerState)

    # Add streaming variants of nodes
    graph.add_node("planner", planner_node_stream)
    graph.add_node("executor", executor_node)
    graph.add_node("summarizer", summarizer_node_stream)

    # Entry point
    graph.set_entry_point("planner")

    # Conditional edge from planner
    graph.add_conditional_edges("planner", _should_continue, {"executor": "executor", "summarizer": "summarizer"})

    # Back to planner after executor
    graph.add_edge("executor", "planner")

    # Summarizer is terminal
    graph.add_edge("summarizer", END)

    return graph.compile()
