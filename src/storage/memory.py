import json

from src.storage.redis_client import redis_client
from src.utils.logging import get_logger

_log = get_logger("memory")

MAX_ROUNDS = 2
TTL = 1800
MAX_TOOL_RESULTS = 10


def _key(user_id: str, session_id: str) -> str:
    return f"copilot:{user_id}:{session_id}"


def _summary_key(user_id: str, session_id: str) -> str:
    return f"copilot:summary:{user_id}:{session_id}"


def _tool_results_key(user_id: str, session_id: str) -> str:
    return f"copilot:tool_results:{user_id}:{session_id}"


def _analysis_key(user_id: str, session_id: str) -> str:
    return f"copilot:analysis:{user_id}:{session_id}"


class SessionMemory:

    @staticmethod
    async def load_history(session_id: str, user_id: str) -> list:
        """Load recent conversation history from Redis (up to MAX_ROUNDS)."""
        val = await redis_client.get(_key(user_id, session_id))
        if val is None:
            _log.debug("load_history  no data  session=%s", session_id)
            return []
        try:
            messages = json.loads(val)
            # Trim to MAX_ROUNDS (each round = user + assistant)
            max_messages = MAX_ROUNDS * 2
            return messages[-max_messages:]
        except (json.JSONDecodeError, TypeError):
            return []

    @staticmethod
    async def save_message(session_id: str, user_id: str, role: str, content: str):
        """Append a message and trim to MAX_ROUNDS."""
        key = _key(user_id, session_id)
        val = await redis_client.get(key)
        if val is None:
            messages = []
        else:
            try:
                messages = json.loads(val)
            except (json.JSONDecodeError, TypeError):
                messages = []

        messages.append({"role": role, "content": content})

        # Trim to MAX_ROUNDS (each round = user + assistant)
        max_messages = MAX_ROUNDS * 2
        if len(messages) > max_messages:
            messages = messages[-max_messages:]

        await redis_client.set(key, json.dumps(messages, ensure_ascii=False), ex=TTL)

    @staticmethod
    async def save_tool_results(session_id: str, user_id: str, tool_results: list[dict]):
        """Append tool results to Redis, keeping only the latest MAX_TOOL_RESULTS entries."""
        key = _tool_results_key(user_id, session_id)
        val = await redis_client.get(key)
        if val is None:
            existing = []
        else:
            try:
                existing = json.loads(val)
            except (json.JSONDecodeError, TypeError):
                existing = []

        existing.extend(tool_results)
        if len(existing) > MAX_TOOL_RESULTS:
            existing = existing[-MAX_TOOL_RESULTS:]

        await redis_client.set(key, json.dumps(existing, ensure_ascii=False), ex=TTL)

    @staticmethod
    async def load_tool_results(session_id: str, user_id: str) -> list[dict]:
        """Load prior tool results from Redis."""
        key = _tool_results_key(user_id, session_id)
        val = await redis_client.get(key)
        if val is None:
            return []
        try:
            return json.loads(val)
        except (json.JSONDecodeError, TypeError):
            return []

    @staticmethod
    async def save_analysis_context(session_id: str, user_id: str, context: dict):
        """Save structured analysis context (RCA metrics, worst region/category, etc.)."""
        key = _analysis_key(user_id, session_id)
        await redis_client.set(key, json.dumps(context, ensure_ascii=False), ex=TTL)

    @staticmethod
    async def load_analysis_context(session_id: str, user_id: str) -> dict | None:
        """Load structured analysis context from Redis."""
        key = _analysis_key(user_id, session_id)
        val = await redis_client.get(key)
        if val is None:
            return None
        try:
            return json.loads(val)
        except (json.JSONDecodeError, TypeError):
            return None

    @staticmethod
    async def clear(session_id: str, user_id: str):
        """Clear conversation history for a session."""
        await redis_client.delete(_key(user_id, session_id))
        await redis_client.delete(_summary_key(user_id, session_id))
        await redis_client.delete(_tool_results_key(user_id, session_id))
        await redis_client.delete(_analysis_key(user_id, session_id))

    @staticmethod
    async def load_summary(session_id: str, user_id: str) -> str | None:
        """Load conversation summary from Redis."""
        val = await redis_client.get(_summary_key(user_id, session_id))
        return val

    @staticmethod
    async def save_summary(session_id: str, user_id: str, summary: str):
        """Save conversation summary to Redis."""
        await redis_client.set(
            _summary_key(user_id, session_id), summary, ex=TTL
        )

    @staticmethod
    async def get_round_count(session_id: str, user_id: str) -> int:
        """Get current number of conversation rounds (user+assistant pairs)."""
        val = await redis_client.get(_key(user_id, session_id))
        if val is None:
            return 0
        try:
            messages = json.loads(val)
            return len([m for m in messages if m.get("role") == "user"])
        except (json.JSONDecodeError, TypeError):
            return 0

    @staticmethod
    async def load_full_context(session_id: str, user_id: str) -> list[dict]:
        """Load full context: [summary_system_msg, ...recent_messages]."""
        summary = await SessionMemory.load_summary(session_id, user_id)
        messages = await SessionMemory.load_history(session_id, user_id)
        _log.debug("load_full_context  summary=%s  msgs=%d", "yes" if summary else "no", len(messages))

        result = []
        if summary:
            result.append({
                "role": "system",
                "content": f"对话摘要（历史上下文）：{summary}",
            })
        result.extend(messages)
        return result
