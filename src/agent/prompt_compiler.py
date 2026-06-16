import json
from collections import Counter
from dataclasses import dataclass

from src.agent.planner_prompt_parts import (
    GLOBAL_PLANNER_PROMPT,
    METRIC_TREND_SQL_RULES,
    SQL_DIAGNOSIS_PRIORITY_TEMPLATE,
)
from src.agent.planner_skills import PlannerSkill, select_planner_skill
from src.agent.sql_utils import extract_user_sql
from src.sql.schema_loader import SCHEMA_TEXT
from src.tools.base import ToolRegistry
from config import (
    REASONING_BACKTRACK_ENABLED,
    REASONING_THOUGHT_STRUCTURE,
    REASONING_TOT_ENABLED,
)


@dataclass(frozen=True)
class PlannerPromptContext:
    user_query: str
    selected_skill: PlannerSkill
    tools_info: str
    schema_text: str
    previous_analysis: str = ""
    tool_results: list[dict] | None = None
    extracted_sql: str | None = None


def build_tools_info() -> str:
    """Build a compact description of registered tools for the planner."""
    import src.tools  # noqa: F401  # register tool instances

    lines = []
    for tool in ToolRegistry.list_all():
        meta = []
        if tool.get("permission_tag"):
            meta.append(f"permission={tool['permission_tag']}")
        if tool.get("timeout"):
            meta.append(f"timeout={tool['timeout']}s")
        meta_text = f" ({', '.join(meta)})" if meta else ""
        lines.append(f"- {tool['name']}{meta_text}: {tool['description']}")
        props = tool["input_schema"].get("properties", {})
        for key, val in props.items():
            desc = val.get("description", "")
            if val.get("enum"):
                desc += f" (可选值: {', '.join(val['enum'])})"
            lines.append(f"    input.{key}: {desc}")
    lines.append("- final_answer: 给出最终自然语言回答")
    return "\n".join(lines)


def build_prompt_context(
    user_query: str = "",
    previous_analysis: str = "",
    tool_results: list[dict] | None = None,
) -> PlannerPromptContext:
    skill = select_planner_skill(user_query)
    return PlannerPromptContext(
        user_query=user_query,
        selected_skill=skill,
        tools_info=build_tools_info(),
        schema_text=SCHEMA_TEXT,
        previous_analysis=previous_analysis,
        tool_results=tool_results or [],
        extracted_sql=extract_user_sql(user_query),
    )


def build_planner_prompt(
    user_query: str = "",
    previous_analysis: str = "",
    tool_results: list[dict] | None = None,
) -> str:
    """Compile global rules, selected skill, and runtime context into one prompt."""
    ctx = build_prompt_context(user_query, previous_analysis, tool_results)
    parts = [
        GLOBAL_PLANNER_PROMPT.strip(),
        _format_skill_block(ctx.selected_skill),
    ]

    if ctx.extracted_sql and ctx.selected_skill.id in {"sql_diagnosis", "sql_optimization"}:
        parts.append(SQL_DIAGNOSIS_PRIORITY_TEMPLATE.format(sql=ctx.extracted_sql).strip())

    if ctx.selected_skill.id == "metric_trend":
        parts.append(METRIC_TREND_SQL_RULES.strip())

    parts.extend([
        _format_schema_block(ctx.schema_text),
        _format_tools_block(ctx.tools_info),
    ])

    history = _format_tool_history(ctx.tool_results or [])
    if history:
        parts.append(history)

    if ctx.previous_analysis:
        parts.append(
            "## 历史分析上下文\n\n"
            "以下是之前几轮对话中已执行的分析结果，请基于这些历史结论决定下一步：\n\n"
            f"{ctx.previous_analysis}"
        )

    return "\n\n".join(part for part in parts if part).strip()


def _format_skill_block(skill: PlannerSkill) -> str:
    return (
        "## 当前 Skill\n\n"
        f"- skill_id: {skill.id}\n"
        f"- 名称: {skill.name}\n"
        f"- 目标: {skill.purpose}\n\n"
        "执行准则：\n"
        f"{skill.instructions}"
    )


def _format_schema_block(schema_text: str) -> str:
    return (
        "## 可用数据表结构\n\n"
        "以下结构仅供生成 SQL 参考；实际元数据问题以 query_metadata 工具返回为准。\n\n"
        f"{schema_text}"
    )


def _format_tools_block(tools_info: str) -> str:
    return f"## 可用工具\n\n{tools_info}"


def _format_tool_history(tool_results: list[dict]) -> str:
    if not tool_results:
        return ""

    call_counts = Counter()
    for result in tool_results:
        tool = result.get("tool", "unknown")
        inp = json.dumps(result.get("input", {}), ensure_ascii=False)
        call_counts[(tool, inp)] += 1

    lines = ["## 已执行的工具调用（不要重复调用）"]
    seen = set()
    for i, result in enumerate(tool_results, 1):
        tool = result.get("tool", "unknown")
        inp = result.get("input", {})
        key = json.dumps({"tool": tool, "input": inp}, ensure_ascii=False)
        if key in seen:
            continue
        seen.add(key)

        input_key = json.dumps(inp, ensure_ascii=False)
        count = call_counts.get((tool, input_key), 1)
        status = "成功" if result.get("success") else "失败"
        repeat_note = f"（已重复调用 {count} 次！）" if count > 1 else ""
        output = str(result.get("output", ""))[:200]
        lines.append(f"{i}. [{tool}] input={input_key} → {status}{repeat_note}\n   返回: {output}")

    lines.append("你已经执行过以上工具。如果数据已足够回答用户问题，输出 final_answer；如果需要新操作，必须使用不同工具或不同输入。")
    return "\n".join(lines)
