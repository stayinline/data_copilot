import time
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any


@dataclass
class ToolResult:
    success: bool
    data: Any
    error: str | None = None
    latency_ms: float = 0.0
    from_cache: bool = False


class Tool(ABC):
    name: str
    description: str
    input_schema: dict

    @abstractmethod
    async def execute(self, input: dict) -> ToolResult:
        ...


class ToolRegistry:
    _tools: dict[str, Tool] = {}

    @classmethod
    def register(cls, tool: Tool):
        cls._tools[tool.name] = tool

    @classmethod
    def get(cls, name: str) -> Tool | None:
        return cls._tools.get(name)

    @classmethod
    def list_all(cls) -> list[dict]:
        return [
            {"name": t.name, "description": t.description, "input_schema": t.input_schema}
            for t in cls._tools.values()
        ]
