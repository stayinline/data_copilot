"""Prompt templates for advanced reasoning capabilities."""

# ─────────────────────────────────────────────
# 1. Explicit Thought Structure
# ─────────────────────────────────────────────
THOUGHT_STRUCTURE_INSTRUCTION = """\
## 思考格式要求

你必须在每次行动前输出显式思考过程。你的输出格式如下：

```json
{
  "thought": "你对当前情况的分析，以及你决定下一步的理由",
  "action": {"name": "工具名", "input": {...}},
  "alternatives": [
    {"thought": "备选方案 A 的思路", "action": {"name": "...", "input": {...}}},
    {"thought": "备选方案 B 的思路", "action": {"name": "...", "input": {...}}}
  ]
}
```

- `thought` 字段：说明你为什么选择这个行动
- `action` 字段：你决定执行的工具调用
- `alternatives` 字段：你考虑过但暂未选择的其他可行方案（0-2 个）

如果你已经有足够信息回答，输出 `{"thought": "...", "name": "final_answer", "content": "..."}`
"""

# ─────────────────────────────────────────────
# 2. Tree of Thoughts — Beam Search
# ─────────────────────────────────────────────
TOT_BEAM_PROMPT_SUFFIX = """\
## 多思路探索要求

请生成 {beam_width} 个不同的下一步候选方案。每个方案必须：
1. 使用不同的工具或不同的输入参数
2. 附带评分（0.0-1.0）和评分理由
3. 按可行性从高到低排序

输出格式：
```json
{{
  "thought": "整体分析",
  "beam_candidates": [
    {{
      "thought": "方案 1 的思路",
      "action": {{"name": "...", "input": {{...}}}},
      "score": 0.9,
      "reasoning": "为什么这个方案最优"
    }},
    {{
      "thought": "方案 2 的思路",
      "action": {{"name": "...", "input": {{...}}}},
      "score": 0.7,
      "reasoning": "为什么这个方案次优"
    }},
    {{
      "thought": "方案 3 的思路",
      "action": {{"name": "...", "input": {{...}}}},
      "score": 0.5,
      "reasoning": "为什么这个方案也可行"
    }}
  ]
}}
```

系统会自动选择得分最高的方案执行。
"""

# ─────────────────────────────────────────────
# 3. Subtask Decomposition
# ─────────────────────────────────────────────
SUBTASK_DECOMPOSE_PROMPT = """\
你是一个任务规划专家。请将用户的复杂查询拆解为多个可执行的子任务。

## 规则

1. 每个子任务应该是独立的、可被工具执行的查询或分析步骤
2. 标注子任务之间的依赖关系（depends_on 为空列表表示可并行执行）
3. 子任务数量不超过 6 个
4. 如果问题很简单（1-2 步即可完成），输出空列表 `"subtasks": []`

## 输出格式

```json
{{
  "subtasks": [
    {{
      "id": 1,
      "description": "获取华东区 Q1 销售数据",
      "depends_on": []
    }},
    {{
      "id": 2,
      "description": "获取华东区去年同期销售数据",
      "depends_on": []
    }},
    {{
      "id": 3,
      "description": "计算同比变化并识别异常品类",
      "depends_on": [1, 2]
    }}
  ]
}}
```

## 用户问题

{user_query}

请输出 JSON：
"""

# ─────────────────────────────────────────────
# 4. Backtracking Retry Context
# ─────────────────────────────────────────────
BACKTRACK_CONTEXT_TEMPLATE = """\
## ⚠️ 失败回溯（重要）

以下操作已经失败，**请勿重复尝试相同的操作**：

{failed_items}

请换一种完全不同的方法来获取信息。可能的替代策略：
- 换一张表查询
- 使用 query_metadata 确认真实的表名和字段名
- 换一种工具（如用 pipeline_full_diagnosis 代替分别查各环节）
- 缩小查询范围或调整过滤条件

当前是第 {current_depth}/{max_depth} 次回溯。如果再次失败，请基于已有数据给出最佳可能的回答。
"""

# ─────────────────────────────────────────────
# 5. Self-Evaluation (for ToT scoring)
# ─────────────────────────────────────────────
SELF_EVALUATE_PROMPT = """\
你是一个评估专家。请评估以下行动方案的可行性并打分（0.0-1.0）。

## 用户问题
{user_query}

## 待评估方案
- 思路: {thought}
- 行动: {action_name}({action_input})

## 已失败操作（避免重复）
{failed_context}

请评分并说明理由。输出格式：
{{"score": 0.X, "reasoning": "评分理由"}}
"""
