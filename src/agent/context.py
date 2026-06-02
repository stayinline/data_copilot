from dataclasses import dataclass, field


@dataclass
class ExecutionContext:
    session_id: str
    user_id: str
    state: str = "planning"  # "planning" | "executing" | "completed" | "failed"
    step_results: list[dict] = field(default_factory=list)
    retry_count: int = 0
    max_retries: int = 3
    tool_call_count: int = 0
    max_tool_calls: int = 3
    conversation_history: list = field(default_factory=list)
    context_summary: str | None = None
    prior_tool_results: list[dict] = field(default_factory=list)
    prior_analysis: dict | None = None
    llm_usage: list[dict] = field(default_factory=list)
    permissions: set[str] | None = None
