import json
import re

from langgraph.graph import StateGraph, END

from src.agent.planner_state import PlannerState
from src.agent.llm_client import chat_completion
from src.agent.sql_fix import auto_fix_sql
from src.agent.prompt_compiler import (
    build_planner_prompt as compile_planner_prompt,
    build_tools_info,
)
from src.agent.sql_utils import extract_user_sql
from src.agent.reasoning_nodes import (
    decompose_node,
    subtask_router,
    parallel_executor,
    check_should_backtrack,
    format_backtrack_context,
)
from src.agent.reasoning_prompts import (
    THOUGHT_STRUCTURE_INSTRUCTION,
    TOT_BEAM_PROMPT_SUFFIX,
    BACKTRACK_CONTEXT_TEMPLATE,
    SELF_EVALUATE_PROMPT,
)
from src.tools.base import ToolExecutionContext, ToolRegistry
from src.sql.validator import SqlValidator
from src.sql.schema_loader import ALLOWED_TABLES
from src.utils.logging import get_logger
from config import (
    CHAT_MODEL,
    PLANNER_MAX_DEPTH,
    PLANNER_MAX_TOOL_CALLS,
    REASONING_THOUGHT_STRUCTURE,
    REASONING_TOT_ENABLED,
    REASONING_SUBTASK_DECOMPOSITION,
    REASONING_BACKTRACK_ENABLED,
    TOT_BEAM_WIDTH,
    BACKTRACK_MAX_DEPTH,
)

_log = get_logger("planner.graph")

_DEFAULT_TOOL_PERMISSIONS = {"sql:read", "metadata:read", "analysis:read", "pipeline:read"}


def _tool_context(state: PlannerState) -> ToolExecutionContext:
    return ToolExecutionContext(
        user_id=state.get("user_id"),
        session_id=state.get("session_id"),
        permissions=state.get("permissions") or _DEFAULT_TOOL_PERMISSIONS,
    )


def _extract_user_sql(query: str) -> str | None:
    return extract_user_sql(query)


def _build_planner_prompt(
    user_query: str = "",
    previous_analysis: str = "",
    tool_results: list[dict] | None = None,
    backtrack_context: str = "",
    tot_suffix: str = "",
) -> str:
    base = compile_planner_prompt(user_query, previous_analysis, tool_results)
    parts = [base]
    if REASONING_THOUGHT_STRUCTURE:
        parts.append(THOUGHT_STRUCTURE_INSTRUCTION.strip())
    if backtrack_context:
        parts.append(backtrack_context)
    if tot_suffix:
        parts.append(tot_suffix)
    return "\n\n".join(p for p in parts if p)


def _build_tools_info() -> str:
    return build_tools_info()


def _extract_json(text: str) -> dict | None:
    """Extract the first balanced JSON object from text."""
    start = text.find("{")
    if start == -1:
        return None
    depth = 0
    for i, ch in enumerate(text[start:], start):
        if ch == "{":
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0:
                candidate = text[start:i + 1]
                try:
                    return json.loads(candidate)
                except json.JSONDecodeError:
                    return None
    return None


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

    # ── Build backtrack context if in backtrack mode ──
    backtrack_context = ""
    if REASONING_BACKTRACK_ENABLED and state.get("backtrack_mode"):
        failed_actions = state.get("failed_actions", [])
        if failed_actions:
            formatted = format_backtrack_context(failed_actions)
            current_depth = state.get("backtrack_depth", 1)
            backtrack_context = BACKTRACK_CONTEXT_TEMPLATE.format(
                failed_items=formatted,
                current_depth=current_depth,
                max_depth=BACKTRACK_MAX_DEPTH,
            )

    # ── Build ToT beam suffix if enabled ──
    tot_suffix = ""
    if REASONING_TOT_ENABLED:
        tot_suffix = TOT_BEAM_PROMPT_SUFFIX.format(beam_width=TOT_BEAM_WIDTH)

    previous_analysis = state.get("previous_analysis", "")
    tool_results = state.get("tool_results", [])
    user_query = state.get("user_query", "")
    messages = [{"role": "system", "content": _build_planner_prompt(user_query, previous_analysis, tool_results, backtrack_context, tot_suffix)}]
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

    # ── Parse the JSON response ──
    parsed = _extract_json(response)

    # ── Try to extract ToT beam candidates ──
    beam_candidates = None
    if REASONING_TOT_ENABLED and parsed and "beam_candidates" in parsed:
        beam_candidates = parsed["beam_candidates"]

    # ── Score beam candidates if present ──
    chosen_action = None
    if beam_candidates and len(beam_candidates) > 0:
        chosen_action = await _score_and_select_beam(beam_candidates, state)

    # ── Try to parse as tool call (explicit action, chosen from beam, or fallback) ──
    tool_call = None

    # Priority 1: explicit "action" field in the parsed JSON
    if parsed and "action" in parsed and isinstance(parsed["action"], dict):
        tool_call = parsed["action"]

    # Priority 2: chosen action from beam scoring
    if not tool_call and chosen_action:
        tool_call = chosen_action

    # Priority 3: fallback — if beam_candidates exist but no action was chosen,
    # use the highest-scored candidate's action (guard against LLM not outputting action)
    if not tool_call and beam_candidates:
        for cand in sorted(beam_candidates, key=lambda x: x.get("score", 0), reverse=True):
            if cand.get("action") and isinstance(cand["action"], dict):
                tool_call = cand["action"]
                _log.debug("planner_node: fallback to top beam candidate action")
                break

    # Priority 4: legacy _extract_tool_call (for non-ToT responses)
    if not tool_call:
        tool_call = _extract_tool_call(response)

    # ── Critical: if tool call failed previously, don't treat as final_answer ──
    # When there are recent failures and the LLM outputs text without a tool call,
    # it's likely asking for confirmation instead of acting. Force retry.
    tool_results = state.get("tool_results", [])
    has_recent_failure = any(not tr.get("success", True) for tr in tool_results[-3:])

    if not tool_call and has_recent_failure:
        # LLM didn't generate a tool call despite recent failures.
        # Force a retry by treating this as a "need more data" situation.
        _log.info("planner_node: recent failure detected, forcing retry despite no tool call")
        # Return empty observation to trigger another planner round
        return {
            "messages": [{"role": "system", "content": "工具调用失败，需要换一种方式获取数据。请生成新的工具调用，不要询问用户确认。"}],
            "observations": ["⚠️ 上次查询失败，请尝试使用不同的表或字段。"],
            "_usage_list": state.get("_usage_list", []) + ([usage] if usage else []),
        }

    # ── Extract thought for history ──
    thought_entry = None
    if REASONING_THOUGHT_STRUCTURE and parsed and "thought" in parsed:
        thought_entry = {
            "thought": parsed["thought"],
            "action": parsed.get("action", chosen_action or {}),
        }

    if tool_call:
        # "final_answer" means done — set final_answer directly
        if tool_call.get("name") == "final_answer":
            delta: dict = {
                "final_answer": tool_call.get("content", response),
                "messages": [{"role": "assistant", "content": response}],
                "_usage_list": state.get("_usage_list", []) + ([usage] if usage else []),
            }
            if thought_entry:
                delta["thought_history"] = [thought_entry]
            return delta

        delta = {
            "messages": [{"role": "assistant", "content": response}],
            "tool_call_count": state["tool_call_count"] + 1,
            "_next_action": tool_call,
            "_usage_list": state.get("_usage_list", []) + ([usage] if usage else []),
        }
        if thought_entry:
            delta["thought_history"] = [thought_entry]
        if beam_candidates:
            delta["beam_candidates"] = beam_candidates
        return delta

    # No valid tool call -> treat as final answer
    delta = {
        "final_answer": response,
        "messages": [{"role": "assistant", "content": response}],
        "_usage_list": state.get("_usage_list", []) + ([usage] if usage else []),
    }
    if thought_entry:
        delta["thought_history"] = [thought_entry]
    return delta


async def _score_and_select_beam(beam_candidates: list[dict], state: PlannerState) -> dict | None:
    """Score beam candidates via self-evaluation and return the best one.

    All candidates are scored in parallel (asyncio.gather) to avoid sequential LLM latency.
    """
    import asyncio

    if not beam_candidates:
        return None

    user_query = state.get("user_query", "")
    failed_actions = state.get("failed_actions", [])

    # Format failed context for scoring
    failed_ctx = "无"
    if failed_actions:
        failed_ctx = format_backtrack_context(
            [{"action_name": fa.get("action_name", ""), "action_input": fa.get("action_input", {}), "error": fa.get("error", "")}
             for fa in failed_actions[-3:]]
        )

    async def _score_one(cand: dict) -> dict:
        """Score a single candidate. Uses pre-score if strong, otherwise calls LLM."""
        thought = cand.get("thought", "")
        action = cand.get("action", {})
        action_name = action.get("name", "unknown")
        action_input = action.get("input", {})
        pre_score = cand.get("score", 0.5)

        # Skip LLM eval if pre-score is already high
        if pre_score >= 0.8:
            cand["score"] = pre_score
            return cand

        try:
            eval_prompt = SELF_EVALUATE_PROMPT.format(
                user_query=user_query,
                thought=thought,
                action_name=action_name,
                action_input=json.dumps(action_input, ensure_ascii=False),
                failed_context=failed_ctx,
            )
            eval_response = await chat_completion(
                [{"role": "user", "content": eval_prompt}],
                model=CHAT_MODEL, temperature=0.1, max_retries=0,
            )
            eval_parsed = _extract_json(eval_response)
            if eval_parsed and "score" in eval_parsed:
                cand["score"] = float(eval_parsed["score"])
            else:
                cand["score"] = pre_score
        except Exception:
            cand["score"] = pre_score
        return cand

    # Score all candidates in parallel
    scored = await asyncio.gather(*[_score_one(c) for c in beam_candidates], return_exceptions=True)

    # Filter out exceptions, fall back to pre-scored
    valid = [s for s in scored if not isinstance(s, Exception)]
    if not valid:
        valid = beam_candidates  # all scored failed, use originals

    # Sort by score descending, pick the best
    valid.sort(key=lambda x: x.get("score", 0), reverse=True)
    best = valid[0]
    return best.get("action")


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
    exec_context = _tool_context(state)

    # Track tool performance
    tool_latency_ms = 0.0
    tool_from_cache = False
    result = None

    if not tool:
        observation = f"Unknown tool: {tool_name}"

    elif tool_name == "run_sql":
        query = tool_input.get("query", "")

        # ── Pre-execution dedup guard: refuse to re-run identical SQL ──
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
                step_result = {
                    "tool": tool_name,
                    "input": tool_input,
                    "output": observation,
                    "success": True,
                    "latency_ms": 0,
                    "from_cache": False,
                    "metadata": {"execution_status": "skipped_by_dedup"},
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
        _user_query = state.get("user_query", "")
        _is_user_sql_diagnosis = False
        if _user_query:
            _extracted = _extract_user_sql(_user_query)
            if _extracted:
                _user_norm = _norm(_extracted)
                _proposed_norm = _norm(query)
                if _user_norm == _proposed_norm or _user_norm.startswith(_proposed_norm) or _proposed_norm.startswith(_user_norm):
                    _is_user_sql_diagnosis = True

        if observation is None:
            result = await ToolRegistry.execute(tool_name, tool_input, exec_context)
            tool_latency_ms = result.latency_ms
            tool_from_cache = result.from_cache
            if result.success:
                observation = json.dumps(result.data, ensure_ascii=False)
            elif _is_user_sql_diagnosis:
                _schema_info = ""
                if ToolRegistry.get("query_metadata"):
                    _matched_table = None
                    for _tbl in sorted(
                        [t for t in ALLOWED_TABLES if re.search(rf"\b{t}\b", query, re.IGNORECASE)],
                        key=len, reverse=True
                    ):
                        _matched_table = _tbl
                        break
                    if _matched_table:
                        try:
                            _meta_result = await ToolRegistry.execute(
                                "query_metadata",
                                {"query_type": "schema", "table_name": _matched_table},
                                exec_context,
                            )
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
                    result2 = await ToolRegistry.execute("run_sql", {"query": fixed_sql}, exec_context)
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
                            "metadata": result2.metadata,
                        })
                    else:
                        observation = f"【SQL 自动修复失败】原始错误：{result.error}。修复后的 SQL 仍然失败：{result2.error}"
                else:
                    observation = f"Error: {result.error}"

    else:
        # Generic tool execution (e.g., query_metadata)
        result = await ToolRegistry.execute(tool_name, tool_input, exec_context)
        tool_latency_ms = result.latency_ms
        tool_from_cache = result.from_cache
        if result.success:
            observation = json.dumps(result.data, ensure_ascii=False)
        else:
            observation = f"Error: {result.error}"

    # ── Backtrack: record failure if execution failed ──
    failed_delta = []
    backtrack_changes = {}
    is_error = observation.startswith("Error:") or "validation failed" in observation

    if REASONING_BACKTRACK_ENABLED and is_error:
        failed_entry = {
            "action_name": tool_name,
            "action_input": tool_input,
            "error": observation[:500],
        }
        failed_delta = [failed_entry]

        should_bt, new_depth = check_should_backtrack(state)
        if should_bt:
            backtrack_changes = {
                "backtrack_mode": True,
                "backtrack_depth": new_depth,
            }
            _log.info("backtrack triggered: depth=%d/%d action=%s", new_depth, BACKTRACK_MAX_DEPTH, tool_name)

    step_result = {
        "tool": tool_name,
        "input": tool_input,
        "output": observation,
        "success": not is_error,
        "latency_ms": round(tool_latency_ms, 1),
        "from_cache": tool_from_cache,
        "metadata": result.metadata if result else {},
    }

    delta = {
        "observations": [observation],
        "tool_results": [step_result],
        "messages": [{"role": "system", "content": f"Observation: {observation}"}],
    }
    if failed_delta:
        delta["failed_actions"] = failed_delta
    delta.update(backtrack_changes)
    return delta


def _should_continue(state: PlannerState) -> str:
    """Conditional edge: check if we should continue or finish."""
    # Storm protection
    if state.get("tool_call_count", 0) >= PLANNER_MAX_TOOL_CALLS:
        return "summarizer"

    # If final_answer already set, finish
    if state.get("final_answer"):
        return "summarizer"

    # ── Backtrack mode: if still have depth, go back to planner with failure context ──
    if state.get("backtrack_mode") and state.get("backtrack_depth", 0) < BACKTRACK_MAX_DEPTH:
        return "planner"

    # ── If there are recent failures, always continue (don't dedup-reject a retry) ──
    tool_results = state.get("tool_results", [])
    has_recent_failure = any(not tr.get("success", True) for tr in tool_results[-3:])

    # Check if planner produced a tool call
    messages = state.get("messages", [])
    if messages:
        last_msg = messages[-1]["content"]
        # Use the same extraction logic as planner_node: check parsed JSON first
        parsed = _extract_json(last_msg)
        tool_call = None
        if parsed and "action" in parsed and isinstance(parsed["action"], dict):
            tool_call = parsed["action"]
        elif parsed and "beam_candidates" in parsed:
            # Fallback: use highest-scored beam candidate
            for cand in sorted(parsed["beam_candidates"], key=lambda x: x.get("score", 0), reverse=True):
                if cand.get("action") and isinstance(cand["action"], dict):
                    tool_call = cand["action"]
                    break
        if not tool_call:
            tool_call = _extract_tool_call(last_msg)

        if tool_call:
            tool_name = tool_call.get("name", "")
            tool_input = tool_call.get("input", {})
            if "name" in tool_input:
                tool_input = {k: v for k, v in tool_input.items() if k != "name"}

            if tool_name == "run_sql":
                # Only dedup if the previous run_sql SUCCEEDED
                # If it failed, we WANT to try a different SQL
                if not has_recent_failure:
                    def _normalize_sql(s: str) -> str:
                        s = s.strip().rstrip(";").strip()
                        s = re.sub(r"\s+LIMIT\s+\d+\s*$", "", s, flags=re.IGNORECASE).strip()
                        return s.upper()

                    proposed_sql = _normalize_sql(tool_input.get("query", ""))
                    for tr in tool_results:
                        if tr.get("tool") != "run_sql":
                            continue
                        if not tr.get("success", True):
                            continue  # Skip failed queries in dedup
                        stored_sql = _normalize_sql((tr.get("input") or {}).get("query", ""))
                        if proposed_sql == stored_sql:
                            _log.info("dedup detected: repeated run_sql call, forcing summarize")
                            return "summarizer"
            else:
                tool_input_normalized = frozenset({k: str(v) for k, v in tool_input.items()}.items())
                same_calls = sum(
                    1 for tr in tool_results
                    if tr.get("tool") == tool_name
                    and tr.get("success", True)  # Only count successful calls
                    and frozenset({k: str(v) for k, v in (tr.get("input") or {}).items()}.items()) == tool_input_normalized
                )
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

    # If we backtracked and still have limited data, acknowledge it
    if state.get("backtrack_mode"):
        summary_rules += "\n\n注意：部分查询尝试了多种方法但仍未能获取完整数据。请基于已有信息给出最佳回答，并说明哪些信息未能获取。"

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
    parsed = _extract_json(response)
    if parsed and "name" in parsed and parsed["name"] != "beam_candidates":
        # Filter out beam_candidates wrapper
        if isinstance(parsed.get("name"), str):
            return parsed
    # Fallback: look for action inside thought-structured output
    if parsed and "action" in parsed:
        return parsed["action"]
    return None


def build_planner_graph():
    """Build and compile the LangGraph planner StateGraph."""
    graph = StateGraph(PlannerState)

    # Add core nodes
    graph.add_node("planner", planner_node)
    graph.add_node("executor", executor_node)
    graph.add_node("summarizer", summarizer_node)

    # ── Conditional entry point based on subtask decomposition flag ──
    if REASONING_SUBTASK_DECOMPOSITION:
        graph.add_node("decompose", decompose_node)
        graph.add_node("parallel_executor", parallel_executor)

        # Entry: decompose first
        graph.set_entry_point("decompose")

        # decompose → conditional edge (route to parallel_executor or planner)
        graph.add_conditional_edges("decompose", subtask_router, {
            "parallel_executor": "parallel_executor",
            "planner": "planner",
        })

        # parallel_executor → planner
        graph.add_edge("parallel_executor", "planner")
    else:
        # Original entry point
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

    # ── Conditional entry point based on subtask decomposition flag ──
    if REASONING_SUBTASK_DECOMPOSITION:
        graph.add_node("decompose", decompose_node)
        graph.add_node("parallel_executor", parallel_executor)

        # Entry: decompose first
        graph.set_entry_point("decompose")

        # decompose → conditional edge (route to parallel_executor or planner)
        graph.add_conditional_edges("decompose", subtask_router, {
            "parallel_executor": "parallel_executor",
            "planner": "planner",
        })

        # parallel_executor → planner
        graph.add_edge("parallel_executor", "planner")
    else:
        # Original entry point
        graph.set_entry_point("planner")

    # Conditional edge from planner
    graph.add_conditional_edges("planner", _should_continue, {"executor": "executor", "summarizer": "summarizer"})

    # Back to planner after executor
    graph.add_edge("executor", "planner")

    # Summarizer is terminal
    graph.add_edge("summarizer", END)

    return graph.compile()
