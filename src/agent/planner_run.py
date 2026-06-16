import asyncio
import json

from src.agent.context import ExecutionContext
from src.agent.planner import build_planner_graph, build_planner_graph_stream
from src.agent.planner_state import PlannerState
from src.utils.logging import get_logger
from config import (
    REASONING_THOUGHT_STRUCTURE,
    REASONING_TOT_ENABLED,
    REASONING_SUBTASK_DECOMPOSITION,
    REASONING_BACKTRACK_ENABLED,
)

_log = get_logger("planner")

# Build graphs once at module load
_planner_graph = build_planner_graph()
_planner_graph_stream = build_planner_graph_stream()


def _format_prior_analysis(prior_analysis: dict | None, prior_tool_results: list[dict]) -> str:
    """Format prior analysis and tool results into a readable string for the planner."""
    parts = []
    if prior_analysis:
        parts.append("### 结构化分析结论")
        for k, v in prior_analysis.items():
            parts.append(f"- {k}: {v}")
    if prior_tool_results:
        parts.append("### 历史工具调用结果")
        for i, tr in enumerate(prior_tool_results, 1):
            tool = tr.get("tool", "unknown")
            output = tr.get("output", "")
            # Truncate long outputs
            if len(output) > 500:
                output = output[:500] + "..."
            parts.append(f"{i}. [{tool}] 输出: {output}")
    return "\n".join(parts)


def _strip_summary(output: str) -> str:
    """Strip 'summary' field from tool result JSON for cleaner frontend display."""
    try:
        data = json.loads(output) if isinstance(output, str) else output
        if isinstance(data, dict) and "summary" in data:
            data.pop("summary")
            return json.dumps(data, ensure_ascii=False)
    except (json.JSONDecodeError, TypeError):
        pass
    return output


async def planner_run(ctx: ExecutionContext, user_message: str) -> str:
    """LangGraph planner entry (blocking). Same signature as react_run."""
    _log.debug("planner_run start  message=%s", user_message)
    previous_analysis = _format_prior_analysis(ctx.prior_analysis, ctx.prior_tool_results)
    initial_state: PlannerState = {
        "user_query": user_message,
        "user_id": ctx.user_id,
        "session_id": ctx.session_id,
        "permissions": ctx.permissions or {"sql:read", "metadata:read", "analysis:read", "pipeline:read"},
        "intent": "",
        "plan": "",
        "current_step_index": 0,
        "observations": [],
        "tool_results": [],
        "final_answer": "",
        "retry_count": 0,
        "tool_call_count": 0,
        "messages": list(ctx.conversation_history) + [{"role": "user", "content": user_message}],
        "error": "",
        "previous_analysis": previous_analysis,
        "_usage_list": [],
        # Advanced reasoning fields (Phase 3)
        "thought_history": [],
        "beam_candidates": [],
        "subtasks": [],
        "current_subtask_index": 0,
        "failed_actions": [],
        "backtrack_mode": False,
        "backtrack_depth": 0,
    }

    result = await _planner_graph.ainvoke(initial_state)
    _log.debug("planner_run done  answer_len=%d", len(result.get("final_answer", "")))

    # Collect step results for context
    for step in result.get("tool_results", []):
        ctx.step_results.append(step)

    # Collect LLM usage
    ctx.llm_usage = result.get("_usage_list", [])

    return result.get("final_answer", "No answer generated")


async def planner_run_stream(ctx: ExecutionContext, user_message: str):
    """LangGraph planner entry (streaming SSE with token-level output)."""
    _log.debug("planner_run_stream start  message=%s", user_message)
    previous_analysis = _format_prior_analysis(ctx.prior_analysis, ctx.prior_tool_results)

    event_queue: asyncio.Queue[dict | None] = asyncio.Queue()
    final_answer_holder: dict[str, str | None] = {"answer": None}
    accumulated_text: list[str] = []  # Use list for mutable closure

    def token_callback(token: str):
        accumulated_text.append(token)
        event_queue.put_nowait({
            "event": "answer_token",
            "data": json.dumps({"text": "".join(accumulated_text)}),
        })

    initial_state: PlannerState = {
        "user_query": user_message,
        "user_id": ctx.user_id,
        "session_id": ctx.session_id,
        "permissions": ctx.permissions or {"sql:read", "metadata:read", "analysis:read", "pipeline:read"},
        "intent": "",
        "plan": "",
        "current_step_index": 0,
        "observations": [],
        "tool_results": [],
        "final_answer": "",
        "retry_count": 0,
        "tool_call_count": 0,
        "messages": list(ctx.conversation_history) + [{"role": "user", "content": user_message}],
        "error": "",
        "previous_analysis": previous_analysis,
        "_token_callback": token_callback,
        "_usage_list": [],
        # Advanced reasoning fields (Phase 3)
        "thought_history": [],
        "beam_candidates": [],
        "subtasks": [],
        "current_subtask_index": 0,
        "failed_actions": [],
        "backtrack_mode": False,
        "backtrack_depth": 0,
    }

    async def drive_graph():
        """Drive the graph and put events in the queue. Signals done with None."""
        usage_list: list[dict] = []
        try:
            async for event in _planner_graph_stream.astream(initial_state):
                for node_name, node_output in event.items():
                    # Collect LLM usage — nodes return cumulative list, take latest when longer
                    node_usage = node_output.get("_usage_list", [])
                    if len(node_usage) > len(usage_list):
                        usage_list = list(node_usage)

                    if node_name == "decompose":
                        subtasks = node_output.get("subtasks", [])
                        if subtasks:
                            event_queue.put_nowait({
                                "event": "subtask_start",
                                "data": json.dumps({
                                    "subtasks": subtasks,
                                    "count": len(subtasks),
                                }),
                            })
                        else:
                            event_queue.put_nowait({
                                "event": "thinking",
                                "data": json.dumps({"message": "任务简单，无需分解"}),
                            })

                    elif node_name == "parallel_executor":
                        tool_results = node_output.get("tool_results", [])
                        for tr in tool_results:
                            event_queue.put_nowait({
                                "event": "subtask_complete",
                                "data": json.dumps({
                                    "subtask_id": tr.get("metadata", {}).get("subtask_id"),
                                    "tool": tr.get("tool"),
                                    "status": "success" if tr.get("success") else "failed",
                                }),
                            })
                            ctx.step_results.append(tr)
                        observations = node_output.get("observations", [])
                        if observations:
                            event_queue.put_nowait({
                                "event": "tool_result",
                                "data": json.dumps({
                                    "tool": "parallel_executor",
                                    "result": "; ".join(observations)[:500],
                                }),
                            })

                    elif node_name == "executor":
                        tool_results = node_output.get("tool_results", [])
                        if tool_results:
                            last_result = tool_results[-1]
                            event_queue.put_nowait({
                                "event": "tool_call",
                                "data": json.dumps({
                                    "tool": last_result.get("tool"),
                                    "input": last_result.get("input"),
                                }),
                            })
                            # Emit sql_fix event if this was an auto-fix
                            if last_result.get("is_sql_fix"):
                                inp = last_result.get("input", {})
                                event_queue.put_nowait({
                                    "event": "sql_fix",
                                    "data": json.dumps({
                                        "original_sql": inp.get("original_sql", ""),
                                        "fixed_sql": inp.get("fixed_sql", ""),
                                    }),
                                })
                            event_queue.put_nowait({
                                "event": "tool_result",
                                "data": json.dumps({
                                    "tool": last_result.get("tool"),
                                    "result": _strip_summary(last_result.get("output", "")),
                                    "latency_ms": last_result.get("latency_ms"),
                                    "from_cache": last_result.get("from_cache"),
                                }),
                            })
                            ctx.step_results.append(last_result)

                        # Emit backtrack event if backtrack_mode was set
                        if node_output.get("backtrack_mode"):
                            failed = node_output.get("failed_actions", [])
                            event_queue.put_nowait({
                                "event": "backtrack",
                                "data": json.dumps({
                                    "depth": node_output.get("backtrack_depth", 1),
                                    "failed_action": failed[-1].get("action_name", "") if failed else "",
                                    "error": failed[-1].get("error", "") if failed else "",
                                }),
                            })

                    elif node_name == "planner":
                        fa = node_output.get("final_answer", "")
                        if fa:
                            final_answer_holder["answer"] = fa
                            ctx.step_results.extend(node_output.get("tool_results", []))
                        else:
                            # Emit thought if present
                            thought_hist = node_output.get("thought_history", [])
                            if thought_hist:
                                for th in thought_hist:
                                    event_queue.put_nowait({
                                        "event": "thought",
                                        "data": json.dumps(th),
                                    })
                            # Emit beam candidates if present
                            beam_cands = node_output.get("beam_candidates", [])
                            if beam_cands:
                                event_queue.put_nowait({
                                    "event": "beam_candidates",
                                    "data": json.dumps({"candidates": beam_cands}),
                                })
                            event_queue.put_nowait({
                                "event": "thinking",
                                "data": json.dumps({
                                    "tool_calls": node_output.get("tool_call_count", 0),
                                }),
                            })

                    elif node_name == "summarizer":
                        if not final_answer_holder["answer"]:
                            fa = node_output.get("final_answer", "")
                            if fa:
                                final_answer_holder["answer"] = fa
                        ctx.step_results.extend(node_output.get("tool_results", []))
        finally:
            ctx.llm_usage = usage_list
            event_queue.put_nowait(None)  # Signal done

    # Run graph in background, drain queue in foreground
    graph_task = asyncio.create_task(drive_graph())

    try:
        while True:
            evt = await event_queue.get()
            if evt is None:
                # Graph finished
                break
            yield evt

            if final_answer_holder["answer"]:
                break
    finally:
        if not graph_task.done():
            graph_task.cancel()
            try:
                await graph_task
            except asyncio.CancelledError:
                pass

    final_answer = final_answer_holder["answer"]
    if final_answer:
        _log.debug("planner_run_stream done  answer_len=%d", len(final_answer))
        yield {"event": "final_answer", "data": json.dumps({"answer": final_answer})}
    else:
        _log.debug("planner_run_stream done (no answer)")
        yield {"event": "final_answer", "data": json.dumps({"answer": "未生成回答"})}
