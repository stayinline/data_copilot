from typing import Annotated, Any, Callable, TypedDict
import operator


class PlannerState(TypedDict, total=False):
    """LangGraph state for the planner.

    List fields use operator.add so LangGraph accumulates node outputs.
    Nodes must return ONLY new items (delta), not the full list.
    """

    user_query: str
    user_id: str
    session_id: str
    permissions: set[str]
    intent: str
    plan: str
    current_step_index: int
    observations: Annotated[list[str], operator.add]
    tool_results: Annotated[list[dict], operator.add]
    final_answer: str
    retry_count: int
    tool_call_count: int
    messages: Annotated[list[dict], operator.add]
    error: str
    _token_callback: Callable[[str], None] | None
    previous_analysis: str
    _usage_list: list[dict]

    # ── Advanced reasoning fields (Phase 3) ──
    thought_history: Annotated[list[dict], operator.add]
    beam_candidates: list[dict]
    subtasks: list[dict]
    current_subtask_index: int
    failed_actions: Annotated[list[dict], operator.add]
    backtrack_mode: bool
    backtrack_depth: int
