"""Data models for advanced reasoning capabilities:
- Tree of Thoughts (beam search candidates)
- Subtask decomposition (DAG tasks)
- Backtracking retry (failure tracking)
"""

from dataclasses import dataclass, field


@dataclass
class ThoughtCandidate:
    """One candidate in a Tree of Thoughts beam search step."""

    thought: str
    action_name: str
    action_input: dict
    score: float = 0.0
    reasoning: str = ""


@dataclass
class Subtask:
    """A subtask produced by the decomposition step."""

    id: int
    description: str
    depends_on: list[int] = field(default_factory=list)
    status: str = "pending"  # pending | running | done | failed
    result: str | None = None


@dataclass
class FailedAction:
    """Tracks a tool execution failure for backtracking."""

    action_name: str
    action_input: dict
    error: str
    attempt: int = 1
    alternatives_tried: list[str] = field(default_factory=list)
