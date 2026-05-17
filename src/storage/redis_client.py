import json
import redis.asyncio as aioredis

from config import REDIS_DSN

redis_client = aioredis.from_url(REDIS_DSN, decode_responses=True)


async def redis_get(key: str, default=None):
    """Get a value from Redis, deserializing JSON if possible."""
    val = await redis_client.get(key)
    if val is None:
        return default
    try:
        return json.loads(val)
    except (json.JSONDecodeError, TypeError):
        return val


async def redis_set(key: str, value, ttl: int = 1800):
    """Set a value in Redis with TTL (default 30 min)."""
    if not isinstance(value, str):
        value = json.dumps(value)
    await redis_client.set(key, value, ex=ttl)


async def redis_delete(key: str):
    """Delete a key from Redis."""
    await redis_client.delete(key)
