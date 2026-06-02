import asyncio
import unittest

from src.tools.base import Tool, ToolExecutionContext, ToolRegistry, ToolResult


class EchoTool(Tool):
    name = "__test_echo"
    description = "Echo input for tool registry tests"
    permission_tag = "test:echo"
    timeout = 1
    input_schema = {
        "type": "object",
        "properties": {
            "message": {"type": "string"},
            "count": {"type": "integer", "default": 1, "minimum": 1, "maximum": 3},
            "mode": {"type": "string", "enum": ["plain", "loud"], "default": "plain"},
        },
        "required": ["message"],
        "additionalProperties": False,
    }

    async def execute(self, input: dict) -> ToolResult:
        return ToolResult(success=True, data=input)


class SlowTool(Tool):
    name = "__test_slow"
    description = "Slow input for timeout tests"
    permission_tag = "test:slow"
    timeout = 0.01
    input_schema = {"type": "object", "properties": {}, "required": []}

    async def execute(self, input: dict) -> ToolResult:
        await asyncio.sleep(0.1)
        return ToolResult(success=True, data={"ok": True})


class ToolRegistryTests(unittest.IsolatedAsyncioTestCase):
    @classmethod
    def setUpClass(cls):
        ToolRegistry.register(EchoTool())
        ToolRegistry.register(SlowTool())

    async def test_execute_validates_input_and_applies_defaults(self):
        result = await ToolRegistry.execute(
            "__test_echo",
            {"message": "hello"},
            ToolExecutionContext(user_id="u1", session_id="s1", permissions={"test:echo"}),
        )

        self.assertTrue(result.success)
        self.assertEqual(result.data, {"message": "hello", "count": 1, "mode": "plain"})
        self.assertEqual(result.metadata["permission_tag"], "test:echo")
        self.assertEqual(result.metadata["execution_status"], "success")
        self.assertTrue(result.metadata["permission_checked"])

    async def test_execute_rejects_invalid_input(self):
        result = await ToolRegistry.execute(
            "__test_echo",
            {"message": "hello", "count": 4, "extra": "nope"},
            ToolExecutionContext(permissions={"test:echo"}),
        )

        self.assertFalse(result.success)
        self.assertIn("Invalid input", result.error)
        self.assertEqual(result.metadata["execution_status"], "validation_failed")

    async def test_execute_rejects_missing_permission(self):
        result = await ToolRegistry.execute(
            "__test_echo",
            {"message": "hello"},
            ToolExecutionContext(permissions={"other:permission"}),
        )

        self.assertFalse(result.success)
        self.assertIn("Permission denied", result.error)
        self.assertEqual(result.metadata["execution_status"], "permission_denied")

    async def test_execute_times_out(self):
        result = await ToolRegistry.execute(
            "__test_slow",
            {},
            ToolExecutionContext(permissions={"test:slow"}),
        )

        self.assertFalse(result.success)
        self.assertIn("timed out", result.error)
        self.assertEqual(result.metadata["execution_status"], "timeout")

    def test_list_all_exposes_execution_metadata(self):
        tools = {tool["name"]: tool for tool in ToolRegistry.list_all()}

        self.assertEqual(tools["__test_echo"]["permission_tag"], "test:echo")
        self.assertEqual(tools["__test_echo"]["timeout"], 1)


if __name__ == "__main__":
    unittest.main()
