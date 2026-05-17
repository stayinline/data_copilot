import json
import re

from src.agent.context import ExecutionContext
from src.tools.base import ToolRegistry
from src.sql.validator import SqlValidator
from src.sql.schema_loader import SCHEMA_TEXT
from src.agent.llm_client import chat_completion
from src.agent.sql_fix import auto_fix_sql
from src.utils.logging import get_logger

_log = get_logger("react")

MAX_ROUNDS = 3


def _format_prior_analysis(prior_analysis: dict | None, prior_tool_results: list[dict]) -> str:
    """Format prior analysis and tool results into a readable string."""
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
            if len(output) > 500:
                output = output[:500] + "..."
            parts.append(f"{i}. [{tool}] 输出: {output}")
    return "\n".join(parts)


from src.tools.base import ToolRegistry


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
    return "\n".join(lines)


def _build_system_prompt(previous_analysis: str = "") -> str:
    tools_info = _build_tools_info()
    prompt = f"""\
你是一个数据平台分析助手。基于工具返回的数据结果，用自然清晰的方式回答用户问题。
要求：
1. 所有结论必须有数据支撑，不编造
2. 先给出核心结论，再补充数据细节和分析说明
3. 适当解读数据背后的含义，如趋势、对比、异常等

重要提示（必须遵守）：
- 当用户询问"有哪些表"、"表结构"、"表字段"等元数据问题时，必须调用 query_metadata 工具，绝对不要写 SQL 查询 system 表，绝对不要直接用 prompt 中的信息直接回答
- 当用户询问业务数据（如 GMV、订单量等）时，调用 run_sql 工具写 SQL 查询
- 当用户询问 Pipeline 排障相关问题（如 Kafka 消费延迟、Flink 任务状态、Pipeline 报错日志、告警记录、数据链路故障等）时，优先调用 pipeline_full_diagnosis 工具进行全链路自动诊断；如果需要针对某个环节深入排查，再调用 pipeline_troubleshoot 工具（operation: check_kafka/check_flink/check_logs/check_alerts）
- **每次只能调用一个工具**，不要同时调用多个工具
- 以下表结构仅供参考，所有元数据查询必须通过 query_metadata 工具

你可查询的表结构如下：
{SCHEMA_TEXT}

可用工具：
{tools_info}

SQL 生成必须基于上述表结构，不要虚构表名或字段名。
如果不确定表结构或字段名，先调用 query_metadata 工具查看 schema。

你需要使用以下格式思考和行动：
Thought: 分析当前情况，决定下一步
Action: 调用工具，以JSON格式输出，例如 {{"name": "run_sql", "input": {{"query": "SELECT ..."}}}}
如果已经收集足够信息，直接输出最终回答，不再调用工具。
"""
    if previous_analysis:
        prompt += f"\n\n## 历史分析上下文\n\n以下是之前几轮对话中已执行的分析结果，请基于这些历史结论决定下一步：\n\n{previous_analysis}\n"
    return prompt


async def call_llm(messages: list[dict], collect_usage: dict | None = None) -> str:
    """Call the LLM API for a completion."""
    return await chat_completion(messages, collect_usage=collect_usage)


def _extract_tool_call(response: str) -> dict | None:
    """Try to extract a tool call JSON from the LLM response."""
    start = response.find('{')
    if start == -1:
        return None
    # Find balanced JSON by counting braces
    depth = 0
    for i, ch in enumerate(response[start:], start):
        if ch == '{':
            depth += 1
        elif ch == '}':
            depth -= 1
            if depth == 0:
                candidate = response[start:i + 1]
                if '"name"' in candidate and 'run_sql' in candidate:
                    try:
                        return json.loads(candidate)
                    except json.JSONDecodeError:
                        pass
                break
    return None


async def react_run(ctx: ExecutionContext, user_message: str) -> str:
    """Standard ReAct loop with up to MAX_ROUNDS iterations."""
    _log.debug("react_run start  message=%s  rounds=%d", user_message, MAX_ROUNDS)
    previous_analysis = _format_prior_analysis(ctx.prior_analysis, ctx.prior_tool_results)
    messages = [{"role": "system", "content": _build_system_prompt(previous_analysis)}]
    messages.extend(ctx.conversation_history)
    messages.append({"role": "user", "content": user_message})

    for round_num in range(MAX_ROUNDS):
        _log.debug("Round %d/%d", round_num + 1, MAX_ROUNDS)
        usage = {}
        response = await call_llm(messages, collect_usage=usage)
        if usage:
            ctx.llm_usage.append(usage)
        _log.debug("LLM response_len=%d", len(response))
        messages.append({"role": "assistant", "content": response})

        tool_call = _extract_tool_call(response)
        if tool_call is None:
            # No tool call found, this is the final answer
            ctx.state = "completed"
            return response

        # Execute tool call
        tool_name = tool_call.get("name", "run_sql")
        tool_input = tool_call.get("input", tool_call)
        if "name" in tool_input:
            tool_input = {k: v for k, v in tool_input.items() if k != "name"}

        observation = None
        tool = ToolRegistry.get(tool_name)
        tool_latency_ms = 0.0
        tool_from_cache = False

        if not tool:
            observation = f"Unknown tool: {tool_name}"
        elif tool_name == "run_sql":
            # Validate SQL before execution
            query = tool_input.get("query", "")
            validation = SqlValidator.validate(query)
            if not validation.success:
                observation = f"SQL validation failed: {'; '.join(validation.errors)}"
            tool_input["query"] = validation.sanitized_sql or query

            if observation is None:
                result = await tool.execute(tool_input)
                tool_latency_ms = result.latency_ms
                tool_from_cache = result.from_cache
                if result.success:
                    observation = json.dumps(result.data, ensure_ascii=False)
                else:
                    # Try SQL auto-fix
                    fixed_sql, fix_status = await auto_fix_sql(
                        original_sql=tool_input.get("query", ""),
                        error_message=result.error,
                    )
                    if fixed_sql:
                        result2 = await tool.execute({"query": fixed_sql})
                        if result2.success:
                            observation = json.dumps(result2.data, ensure_ascii=False)
                            tool_input["query"] = fixed_sql
                            ctx.step_results.append({
                                "tool": "sql_auto_fix",
                                "input": {"original_sql": tool_input.get("query", ""), "fixed_sql": fixed_sql},
                                "output": "SQL auto-fix succeeded",
                                "success": True,
                                "is_sql_fix": True,
                                "latency_ms": round(result2.latency_ms, 1),
                                "from_cache": result2.from_cache,
                            })
                        else:
                            observation = f"Error: {result2.error}"
                    else:
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

        ctx.step_results.append({
            "tool": tool_name,
            "input": tool_input,
            "output": observation,
            "success": not observation.startswith("Error:") and "validation failed" not in observation,
            "latency_ms": round(tool_latency_ms, 1),
            "from_cache": tool_from_cache,
        })

        messages.append({"role": "system", "content": f"Observation: {observation}"})

    # If we exhausted all rounds, return the last response
    ctx.state = "completed"
    return response


async def react_run_stream(ctx: ExecutionContext, user_message: str):
    """Streaming ReAct loop that yields SSE events."""
    _log.debug("react_run_stream start  message=%s", user_message)
    previous_analysis = _format_prior_analysis(ctx.prior_analysis, ctx.prior_tool_results)
    messages = [{"role": "system", "content": _build_system_prompt(previous_analysis)}]

    for round_num in range(MAX_ROUNDS):
        _log.debug("Round %d/%d", round_num + 1, MAX_ROUNDS)
        yield {"event": "thinking", "data": json.dumps({"round": round_num + 1, "total": MAX_ROUNDS})}

        usage = {}
        response = await call_llm(messages, collect_usage=usage)
        if usage:
            ctx.llm_usage.append(usage)
        messages.append({"role": "assistant", "content": response})

        tool_call = _extract_tool_call(response)
        if tool_call is None:
            yield {"event": "final_answer", "data": json.dumps({"answer": response})}
            ctx.state = "completed"
            return

        # Execute tool call
        tool_name = tool_call.get("name", "run_sql")
        tool_input = tool_call.get("input", tool_call)
        if "name" in tool_input:
            tool_input = {k: v for k, v in tool_input.items() if k != "name"}

        yield {"event": "tool_call", "data": json.dumps({"tool": tool_name, "input": tool_input})}

        observation = None
        tool = ToolRegistry.get(tool_name)
        tool_latency_ms = 0.0
        tool_from_cache = False

        if not tool:
            observation = f"Unknown tool: {tool_name}"
        elif tool_name == "run_sql":
            query = tool_input.get("query", "")
            validation = SqlValidator.validate(query)
            if not validation.success:
                observation = f"SQL validation failed: {'; '.join(validation.errors)}"
            tool_input["query"] = validation.sanitized_sql or query

            if observation is None:
                result = await tool.execute(tool_input)
                tool_latency_ms = result.latency_ms
                tool_from_cache = result.from_cache
                if result.success:
                    observation = json.dumps(result.data, ensure_ascii=False)
                else:
                    # Try SQL auto-fix
                    fixed_sql, fix_status = await auto_fix_sql(
                        original_sql=tool_input.get("query", ""),
                        error_message=result.error,
                    )
                    if fixed_sql:
                        result2 = await tool.execute({"query": fixed_sql})
                        if result2.success:
                            observation = json.dumps(result2.data, ensure_ascii=False)
                            tool_input["query"] = fixed_sql
                            sql_fix_result = {
                                "tool": "sql_auto_fix",
                                "input": {"original_sql": tool_input.get("query", ""), "fixed_sql": fixed_sql},
                                "output": "SQL auto-fix succeeded",
                                "success": True,
                                "is_sql_fix": True,
                                "latency_ms": round(result2.latency_ms, 1),
                                "from_cache": result2.from_cache,
                            }
                            ctx.step_results.append(sql_fix_result)
                            yield {"event": "sql_fix", "data": json.dumps({
                                "original_sql": tool_input.get("query", ""),
                                "fixed_sql": fixed_sql,
                                "error": result.error,
                            })}
                        else:
                            observation = f"Error: {result2.error}"
                    else:
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

        yield {"event": "tool_result", "data": json.dumps({
            "tool": tool_name,
            "result": observation,
            "latency_ms": round(tool_latency_ms, 1),
            "from_cache": tool_from_cache,
        })}

        ctx.step_results.append({
            "tool": tool_name,
            "input": tool_input,
            "output": observation,
            "success": not observation.startswith("Error:") and "failed" not in observation.lower(),
            "latency_ms": round(tool_latency_ms, 1),
            "from_cache": tool_from_cache,
        })

        messages.append({"role": "system", "content": f"Observation: {observation}"})

    ctx.state = "completed"
    yield {"event": "final_answer", "data": json.dumps({"answer": response})}
