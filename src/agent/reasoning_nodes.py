"""New LangGraph nodes for advanced reasoning:
- decompose_node: LLM-based subtask decomposition
- subtask_router: conditional edge routing
- parallel_executor: asyncio.gather for independent subtasks
- backtrack_handler: failure tracking and backtrack mode management
"""

import json
import re

from src.agent.llm_client import chat_completion
from src.agent.reasoning_prompts import SUBTASK_DECOMPOSE_PROMPT, BACKTRACK_CONTEXT_TEMPLATE
from src.agent.reasoning_types import Subtask, FailedAction
from src.agent.planner_state import PlannerState
from src.utils.logging import get_logger

from config import (
    CHAT_MODEL,
    REASONING_BACKTRACK_ENABLED,
    BACKTRACK_MAX_DEPTH,
)

_log = get_logger("reasoning_nodes")


def _extract_json(text: str) -> dict | None:
    """Extract the first JSON object from LLM output text."""
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
                try:
                    return json.loads(text[start:i + 1])
                except json.JSONDecodeError:
                    return None
    return None


async def decompose_node(state: PlannerState) -> dict:
    """LLM-based subtask decomposition.

    Analyzes the user query and breaks it into a DAG of subtasks.
    Independent subtasks (depends_on=[]) can be executed in parallel.
    """
    user_query = state.get("user_query", "")
    if not user_query:
        return {"subtasks": []}

    prompt = SUBTASK_DECOMPOSE_PROMPT.format(user_query=user_query)
    messages = [{"role": "user", "content": prompt}]

    try:
        response = await chat_completion(messages, model=CHAT_MODEL, temperature=0.2, max_retries=1)
        parsed = _extract_json(response)
        if not parsed or "subtasks" not in parsed:
            _log.debug("decompose_node: LLM returned invalid JSON, falling back to no decomposition")
            return {"subtasks": []}

        subtasks_raw = parsed["subtasks"]
        if not isinstance(subtasks_raw, list):
            return {"subtasks": []}

        # Validate and normalize
        subtasks = []
        for item in subtasks_raw:
            if not isinstance(item, dict):
                continue
            st = Subtask(
                id=int(item.get("id", len(subtasks) + 1)),
                description=str(item.get("description", "")),
                depends_on=item.get("depends_on", []),
                status="pending",
            )
            subtasks.append(st)

        # Safety: if LLM returns too many subtasks, cap at 6
        if len(subtasks) > 6:
            subtasks = subtasks[:6]

        if not subtasks:
            return {"subtasks": []}

        _log.info("decompose_node: split into %d subtasks", len(subtasks))
        return {
            "subtasks": [
                {"id": s.id, "description": s.description, "depends_on": s.depends_on, "status": s.status}
                for s in subtasks
            ],
            "messages": [{"role": "assistant", "content": f"[Task decomposition] Split into {len(subtasks)} subtasks"}],
        }

    except Exception as e:
        _log.error("decompose_node LLM error: %s", str(e))
        return {"subtasks": []}


def subtask_router(state: PlannerState) -> str:
    """Conditional edge after decompose_node.

    Returns:
        - "parallel_executor" if there are independent subtasks (depends_on=[])
        - "planner" otherwise (simple query or all subtasks have dependencies)
    """
    subtasks = state.get("subtasks", [])
    if not subtasks:
        return "planner"

    has_parallel = any(
        isinstance(t, dict) and not t.get("depends_on", [])
        for t in subtasks
    )
    return "parallel_executor" if has_parallel else "planner"


async def parallel_executor(state: PlannerState) -> dict:
    """Execute independent subtasks in parallel using asyncio.gather.

    For subtasks with depends_on=[], we execute them concurrently.
    Subtasks with dependencies are deferred to the planner loop.
    """
    import asyncio

    from src.agent.context import ExecutionContext
    from src.agent.llm_client import chat_completion
    from src.tools.base import ToolExecutionContext, ToolRegistry
    from config import CHAT_MODEL

    subtasks = state.get("subtasks", [])
    if not subtasks:
        return {}

    user_query = state.get("user_query", "")
    user_id = state.get("user_id", "")
    session_id = state.get("session_id", "")
    permissions = state.get("permissions", {"sql:read", "metadata:read", "analysis:read", "pipeline:read"})

    # Find independent subtasks (depends_on=[])
    independent = [t for t in subtasks if isinstance(t, dict) and not t.get("depends_on", [])]
    if not independent:
        return {}

    exec_ctx = ToolExecutionContext(
        user_id=user_id,
        session_id=session_id,
        permissions=set(permissions) if isinstance(permissions, set) else set(),
    )

    _log.info("parallel_executor: executing %d independent subtasks in parallel", len(independent))

    async def _generate_sql_for_subtask(subtask: dict) -> tuple[str, dict] | None:
        """Ask LLM to generate SQL for a data-query subtask. Returns (sql, tool_input) or None."""
        from src.sql.schema_loader import SCHEMA_TEXT, ALLOWED_TABLES
        sql_prompt = (
            f"用户问题: {user_query}\n\n"
            f"子任务: {subtask.get('description')}\n\n"
            f"可用表结构:\n{SCHEMA_TEXT}\n\n"
            f"请生成一条 ClickHouse SQL 查询来完成这个子任务。\n"
            f"要求:\n"
            f"1. 只使用上述表结构中的表名和字段名，不要虚构\n"
            f"2. 如果不确定字段名，先选择已知可用的\n"
            f"3. 只输出 SQL，不要多余解释\n"
            f"4. 使用 DATE 字段做时间过滤，不要用 order_time（该字段不存在）"
        )
        try:
            sql_response = await chat_completion(
                [{"role": "user", "content": sql_prompt}],
                model=CHAT_MODEL, temperature=0.1, max_retries=1,
            )
            # Strip markdown code blocks if present
            sql_response = re.sub(r"```sql\s*", "", sql_response)
            sql_response = re.sub(r"```\s*", "", sql_response).strip()
            if sql_response:
                return ("run_sql", {"query": sql_response})
        except Exception as e:
            _log.error("parallel_executor: SQL generation failed for subtask %d: %s", subtask.get("id", "?"), e)
        return None

    async def _execute_subtask(subtask: dict) -> dict:
        """Translate a subtask description into tool calls and execute."""
        try:
            tool_name = None
            tool_input = None

            # Simple heuristic to route subtask to the right tool
            desc = subtask.get("description", "").lower()

            if any(kw in desc for kw in ("查", "查询", "获取", "数据", "统计", "gmv", "销售", "订单", "趋势", "同比", "环比")):
                result = await _generate_sql_for_subtask(subtask)
                if result:
                    tool_name, tool_input = result
                else:
                    return {"subtask_id": subtask["id"], "status": "failed", "error": "SQL generation failed"}

            elif any(kw in desc for kw in ("schema", "表结构", "字段", "元数据", "ddl")):
                tool_name = "query_metadata"
                tool_input = {"query_type": "list_tables"}

            elif any(kw in desc for kw in ("归因", "分析", "原因", "下降", "异常")):
                tool_name = "root_cause_analysis"
                tool_input = {"metric": "GMV"}

            elif any(kw in desc for kw in ("pipeline", "链路", "flink", "kafka", "日志", "告警")):
                tool_name = "pipeline_full_diagnosis"
                tool_input = {}

            if not tool_name or not tool_input:
                return {"subtask_id": subtask["id"], "status": "failed", "error": "Could not determine tool"}

            tool = ToolRegistry.get(tool_name)
            if not tool:
                return {"subtask_id": subtask["id"], "status": "failed", "error": f"Unknown tool: {tool_name}"}

            result = await ToolRegistry.execute(tool_name, tool_input, exec_ctx)
            output = json.dumps(result.data, ensure_ascii=False) if result.success else f"Error: {result.error}"
            return {
                "subtask_id": subtask["id"],
                "status": "success" if result.success else "failed",
                "result": output[:2000],
                "tool": tool_name,
                "latency_ms": result.latency_ms,
            }
        except Exception as e:
            _log.error("parallel_executor subtask %d exception: %s", subtask.get("id", "?"), e)
            return {"subtask_id": subtask.get("id", "?"), "status": "failed", "error": str(e)}

    # Execute all independent subtasks in parallel with timeout
    tasks = [_execute_subtask(st) for st in independent]
    try:
        results = await asyncio.gather(*tasks, return_exceptions=True)
    except Exception as e:
        _log.error("parallel_executor gather failed: %s", e)
        # Fallback: run sequentially
        results = []
        for t in tasks:
            try:
                results.append(await t)
            except Exception as e:
                results.append(Exception(f"sequential fallback: {e}"))

    # Update subtask statuses and collect observations
    updated_subtasks = list(subtasks)
    observations = []
    tool_results_delta = []

    for result in results:
        if isinstance(result, Exception):
            _log.error("parallel_executor subtask exception: %s", result)
            continue

        if not isinstance(result, dict):
            continue

        subtask_id = result.get("subtask_id")
        status = result.get("status", "failed")

        # Update subtask status
        for i, st in enumerate(updated_subtasks):
            if isinstance(st, dict) and st.get("id") == subtask_id:
                updated_subtasks[i] = {**st, "status": status, "result": result.get("result", "")}
                break

        obs = f"[子任务 {subtask_id}] {status}: {result.get('result', result.get('error', ''))[:500]}"
        observations.append(obs)

        tool_results_delta.append({
            "tool": result.get("tool", "subtask"),
            "input": {"subtask_id": subtask_id, "description": next(
                (st.get("description", "") for st in independent if st.get("id") == subtask_id), "")},
            "output": result.get("result", result.get("error", "")),
            "success": status == "success",
            "latency_ms": result.get("latency_ms", 0),
            "from_cache": False,
            "metadata": {"subtask_id": subtask_id},
        })

    _log.info("parallel_executor: completed %d subtasks", len(results))
    return {
        "subtasks": updated_subtasks,
        "observations": observations,
        "tool_results": tool_results_delta,
        "messages": [{"role": "system", "content": f"Observation: {'; '.join(observations)}"}],
    }


def format_backtrack_context(failed_actions: list[dict]) -> str:
    """Format failed actions into a readable prompt suffix for backtracking."""
    if not failed_actions:
        return ""

    items = []
    for i, fa in enumerate(failed_actions, 1):
        action_name = fa.get("action_name", "unknown")
        action_input = fa.get("action_input", {})
        error = fa.get("error", "Unknown error")

        # Truncate input for readability
        input_str = json.dumps(action_input, ensure_ascii=False)
        if len(input_str) > 200:
            input_str = input_str[:200] + "..."

        items.append(f"{i}. {action_name}({input_str}) → 错误: {error}")

    return "\n".join(items)


def check_should_backtrack(state: PlannerState) -> tuple[bool, int]:
    """Determine if we should enter backtrack mode.

    Returns (should_backtrack, new_depth).
    Triggers when:
    - REASONING_BACKTRACK_ENABLED is True
    - There are recent failures
    - backtrack_depth < BACKTRACK_MAX_DEPTH
    """
    if not REASONING_BACKTRACK_ENABLED:
        return False, 0

    failed_actions = state.get("failed_actions", [])
    current_depth = state.get("backtrack_depth", 0)

    if not failed_actions:
        return False, 0

    if current_depth >= BACKTRACK_MAX_DEPTH:
        return False, current_depth

    # Check if the last execution failed
    tool_results = state.get("tool_results", [])
    if not tool_results:
        return False, current_depth

    last_result = tool_results[-1]
    if not last_result.get("success", True):
        return True, current_depth + 1

    return False, current_depth
