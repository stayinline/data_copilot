import time
from datetime import date, datetime
from decimal import Decimal

import clickhouse_connect

from src.tools.base import Tool, ToolResult, ToolRegistry
from src.sql.validator import SqlValidator
from src.gateway.metrics import record_tool_call, record_cache_hit, record_cache_miss
from src.cache.query_cache import QueryCache
from src.utils.logging import get_logger
from config import (
    CLICKHOUSE_HOST,
    CLICKHOUSE_PORT,
    CLICKHOUSE_USER,
    CLICKHOUSE_PASSWORD,
    CLICKHOUSE_DATABASE,
    QUERY_CACHE_TTL,
)

_log = get_logger("tool.run_sql")


class RunSqlTool(Tool):
    name = "run_sql"
    description = "Execute read-only SQL SELECT queries against ClickHouse"
    permission_tag = "sql:read"
    timeout = 30
    input_schema = {
        "type": "object",
        "properties": {
            "query": {"type": "string", "description": "SQL SELECT statement"},
            "limit": {"type": "integer", "default": 1000, "maximum": 10000},
        },
        "required": ["query"],
    }

    def _get_client(self):
        return clickhouse_connect.get_client(
            host=CLICKHOUSE_HOST,
            port=CLICKHOUSE_PORT,
            username=CLICKHOUSE_USER,
            password=CLICKHOUSE_PASSWORD,
            database=CLICKHOUSE_DATABASE,
        )

    async def execute(self, input: dict) -> ToolResult:
        start = time.monotonic()
        query = input.get("query", "")
        _log.debug("execute  query_len=%d", len(query))

        # Check QueryCache
        cached = await QueryCache.get(query)
        if cached is not None:
            _log.debug("cache hit  query_len=%d", len(query))
            record_cache_hit("query")
            elapsed = (time.monotonic() - start) * 1000
            record_tool_call("run_sql", "success", elapsed / 1000)
            return ToolResult(
                success=True, data=cached["data"], latency_ms=elapsed, from_cache=True
            )

        record_cache_miss("query")
        _log.debug("cache miss, executing query")

        # Validate SQL
        validation = SqlValidator.validate(query)
        if not validation.success:
            elapsed = (time.monotonic() - start) * 1000
            record_tool_call("run_sql", "failed", elapsed / 1000)
            return ToolResult(success=False, data=None, error="; ".join(validation.errors), latency_ms=elapsed)

        sanitized_query = validation.sanitized_sql or query

        try:
            client = self._get_client()
            result = client.query(sanitized_query)

            def _convert(val):
                if isinstance(val, Decimal):
                    return float(val)
                if isinstance(val, datetime):
                    return val.strftime("%Y-%m-%d %H:%M:%S")
                if isinstance(val, date):
                    return val.strftime("%Y-%m-%d")
                return val

            rows = [
                {col: _convert(v) for col, v in zip(result.column_names, row)}
                for row in result.result_rows
            ]
            client.close()

            data = {"columns": result.column_names, "rows": rows}
            # Write to cache
            await QueryCache.set(query, data, ttl=QUERY_CACHE_TTL)
            _log.debug("success  rows=%d", len(rows))

            elapsed = (time.monotonic() - start) * 1000
            record_tool_call("run_sql", "success", elapsed / 1000)
            return ToolResult(success=True, data=data, latency_ms=elapsed)
        except Exception as e:
            _log.debug("failed  error=%s", str(e)[:200])
            elapsed = (time.monotonic() - start) * 1000
            record_tool_call("run_sql", "failed", elapsed / 1000)
            return ToolResult(success=False, data=None, error=str(e), latency_ms=elapsed)


# Register the tool
ToolRegistry.register(RunSqlTool())
