import hashlib
import json

from src.storage.redis_client import redis_client


def _hash_key(text: str) -> str:
    """Generate SHA256 hash for cache key."""
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


class QueryCache:
    """Cache for SQL query results with configurable TTL."""

    _PREFIX = "copilot:query_cache:"

    @staticmethod
    async def get(sql: str) -> dict | None:
        """Get cached SQL result. Returns dict with 'data' key or None."""
        key = QueryCache._PREFIX + _hash_key(sql)
        val = await redis_client.get(key)
        if val is None:
            return None
        try:
            return json.loads(val)
        except (json.JSONDecodeError, TypeError):
            return None

    @staticmethod
    async def set(sql: str, result: dict, ttl: int = 300):
        """Cache SQL query result."""
        key = QueryCache._PREFIX + _hash_key(sql)
        await redis_client.set(key, json.dumps({"data": result}, ensure_ascii=False), ex=ttl)

    @staticmethod
    async def invalidate(sql: str):
        """Remove cached SQL result."""
        key = QueryCache._PREFIX + _hash_key(sql)
        await redis_client.delete(key)


class RetrievalCache:
    """Cache for metadata retrieval (table schemas, column info, etc.)."""

    _PREFIX = "copilot:retrieval_cache:"

    @staticmethod
    async def get(table: str, type: str = "schema") -> dict | None:
        """Get cached retrieval data."""
        key = RetrievalCache._PREFIX + _hash_key(f"{table}:{type}")
        val = await redis_client.get(key)
        if val is None:
            return None
        try:
            return json.loads(val)
        except (json.JSONDecodeError, TypeError):
            return None

    @staticmethod
    async def set(table: str, type: str, result: dict, ttl: int = 1800):
        """Cache retrieval data."""
        key = RetrievalCache._PREFIX + _hash_key(f"{table}:{type}")
        await redis_client.set(key, json.dumps(result, ensure_ascii=False), ex=ttl)
