import asyncio
import copy
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any


@dataclass
class ToolResult:
    success: bool
    data: Any
    error: str | None = None
    latency_ms: float = 0.0
    from_cache: bool = False
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class ToolExecutionContext:
    user_id: str | None = None
    session_id: str | None = None
    trace_id: str | None = None
    permissions: set[str] | None = None


class Tool(ABC):
    name: str
    description: str
    input_schema: dict
    permission_tag: str = ""
    timeout: float | None = 30

    @abstractmethod
    async def execute(self, input: dict) -> ToolResult:
        ...


class ToolInputValidationError(ValueError):
    def __init__(self, errors: list[str]):
        super().__init__("; ".join(errors))
        self.errors = errors


def _path(parent: str, key: str) -> str:
    return f"{parent}.{key}" if parent else key


def _matches_type(value: Any, expected: str) -> bool:
    if expected == "string":
        return isinstance(value, str)
    if expected == "integer":
        return isinstance(value, int) and not isinstance(value, bool)
    if expected == "number":
        return isinstance(value, (int, float)) and not isinstance(value, bool)
    if expected == "boolean":
        return isinstance(value, bool)
    if expected == "object":
        return isinstance(value, dict)
    if expected == "array":
        return isinstance(value, list)
    if expected == "null":
        return value is None
    return True


def _validate_value(value: Any, schema: dict, path: str) -> tuple[Any, list[str]]:
    errors: list[str] = []
    expected_type = schema.get("type")

    if isinstance(expected_type, list):
        if not any(_matches_type(value, t) for t in expected_type):
            errors.append(f"{path} must be one of types {expected_type}")
            return value, errors
    elif expected_type and not _matches_type(value, expected_type):
        errors.append(f"{path} must be {expected_type}")
        return value, errors

    if "enum" in schema and value not in schema["enum"]:
        errors.append(f"{path} must be one of {schema['enum']}")

    if isinstance(value, (int, float)) and not isinstance(value, bool):
        maximum = schema.get("maximum", schema.get("max"))
        minimum = schema.get("minimum", schema.get("min"))
        if maximum is not None and value > maximum:
            errors.append(f"{path} must be <= {maximum}")
        if minimum is not None and value < minimum:
            errors.append(f"{path} must be >= {minimum}")

    if isinstance(value, str):
        max_length = schema.get("maxLength")
        min_length = schema.get("minLength")
        if max_length is not None and len(value) > max_length:
            errors.append(f"{path} length must be <= {max_length}")
        if min_length is not None and len(value) < min_length:
            errors.append(f"{path} length must be >= {min_length}")

    if isinstance(value, dict) and schema.get("properties"):
        value, child_errors = _validate_object(value, schema, path)
        errors.extend(child_errors)

    if isinstance(value, list) and schema.get("items"):
        item_schema = schema["items"]
        validated_items = []
        for i, item in enumerate(value):
            validated_item, item_errors = _validate_value(item, item_schema, f"{path}[{i}]")
            validated_items.append(validated_item)
            errors.extend(item_errors)
        value = validated_items

    return value, errors


def _validate_object(input_data: dict, schema: dict, path: str = "input") -> tuple[dict, list[str]]:
    errors: list[str] = []
    properties = schema.get("properties", {})
    required = set(schema.get("required", []))
    allow_extra = schema.get("additionalProperties", True)
    validated = dict(input_data)

    for key, prop_schema in properties.items():
        if key not in validated and "default" in prop_schema:
            validated[key] = copy.deepcopy(prop_schema["default"])

    for key in required:
        if key not in validated:
            errors.append(f"{_path(path, key)} is required")

    for key, value in list(validated.items()):
        child_path = _path(path, key)
        prop_schema = properties.get(key)
        if prop_schema is None:
            if not allow_extra:
                errors.append(f"{child_path} is not allowed")
            continue
        validated_value, value_errors = _validate_value(value, prop_schema, child_path)
        validated[key] = validated_value
        errors.extend(value_errors)

    return validated, errors


class ToolRegistry:
    _tools: dict[str, Tool] = {}

    @classmethod
    def register(cls, tool: Tool):
        cls._tools[tool.name] = tool

    @classmethod
    def get(cls, name: str) -> Tool | None:
        return cls._tools.get(name)

    @classmethod
    def validate_input(cls, tool: Tool, input: dict) -> dict:
        if not isinstance(input, dict):
            raise ToolInputValidationError(["input must be object"])

        schema = tool.input_schema or {"type": "object"}
        expected_type = schema.get("type", "object")
        if expected_type != "object":
            raise ToolInputValidationError(["tool input_schema must describe an object"])

        validated, errors = _validate_object(input, schema)
        if errors:
            raise ToolInputValidationError(errors)
        return validated

    @classmethod
    async def execute(
        cls,
        name: str,
        input: dict,
        context: ToolExecutionContext | None = None,
    ) -> ToolResult:
        start = time.monotonic()
        tool = cls.get(name)
        if not tool:
            return ToolResult(
                success=False,
                data=None,
                error=f"Unknown tool: {name}",
                latency_ms=0.0,
                metadata={"tool_name": name, "execution_status": "unknown_tool"},
            )

        permission_checked = bool(context and context.permissions is not None)
        metadata = {
            "tool_name": tool.name,
            "permission_tag": getattr(tool, "permission_tag", ""),
            "timeout_seconds": getattr(tool, "timeout", None),
            "permission_checked": permission_checked,
        }
        if context:
            metadata.update({
                "user_id": context.user_id,
                "session_id": context.session_id,
                "trace_id": context.trace_id,
            })

        permission_tag = getattr(tool, "permission_tag", "")
        if permission_checked and permission_tag:
            permissions = context.permissions or set()
            if "*" not in permissions and permission_tag not in permissions:
                elapsed = (time.monotonic() - start) * 1000
                return ToolResult(
                    success=False,
                    data=None,
                    error=f"Permission denied: tool '{tool.name}' requires '{permission_tag}'",
                    latency_ms=elapsed,
                    metadata={**metadata, "execution_status": "permission_denied"},
                )

        try:
            validated_input = cls.validate_input(tool, input)
        except ToolInputValidationError as exc:
            elapsed = (time.monotonic() - start) * 1000
            return ToolResult(
                success=False,
                data=None,
                error=f"Invalid input: {'; '.join(exc.errors)}",
                latency_ms=elapsed,
                metadata={**metadata, "execution_status": "validation_failed", "validation_errors": exc.errors},
            )

        try:
            timeout = getattr(tool, "timeout", None)
            if timeout and timeout > 0:
                result = await asyncio.wait_for(tool.execute(validated_input), timeout=timeout)
            else:
                result = await tool.execute(validated_input)
        except asyncio.TimeoutError:
            elapsed = (time.monotonic() - start) * 1000
            return ToolResult(
                success=False,
                data=None,
                error=f"Tool '{tool.name}' timed out after {getattr(tool, 'timeout', None)}s",
                latency_ms=elapsed,
                metadata={**metadata, "execution_status": "timeout", "timed_out": True},
            )
        except Exception as exc:
            elapsed = (time.monotonic() - start) * 1000
            return ToolResult(
                success=False,
                data=None,
                error=str(exc),
                latency_ms=elapsed,
                metadata={**metadata, "execution_status": "error"},
            )

        elapsed = (time.monotonic() - start) * 1000
        if result.latency_ms <= 0:
            result.latency_ms = elapsed
        result.metadata = {
            **metadata,
            **(result.metadata or {}),
            "execution_status": "success" if result.success else "failed",
            "wall_latency_ms": round(elapsed, 1),
        }
        return result

    @classmethod
    def list_all(cls) -> list[dict]:
        return [
            {
                "name": t.name,
                "description": t.description,
                "input_schema": t.input_schema,
                "permission_tag": getattr(t, "permission_tag", ""),
                "timeout": getattr(t, "timeout", None),
            }
            for t in cls._tools.values()
        ]
