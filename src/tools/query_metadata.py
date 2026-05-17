"""
query_metadata tool — query ClickHouse system tables for table schemas,
column details, and DDL. Returns structured metadata for the LLM to
understand available tables and their structure.
"""

import time
from dataclasses import dataclass

import clickhouse_connect

from src.tools.base import Tool, ToolResult, ToolRegistry
from src.gateway.metrics import record_tool_call
from src.utils.logging import get_logger
from config import (
    CLICKHOUSE_HOST,
    CLICKHOUSE_PORT,
    CLICKHOUSE_USER,
    CLICKHOUSE_PASSWORD,
    CLICKHOUSE_DATABASE,
)

_log = get_logger("tool.query_metadata")

# Sub-queries supported by this tool
QUERY_TYPES = {
    "list_tables": "List all available tables in the database",
    "schema": "Get columns for a specific table",
    "ddl": "Get CREATE TABLE statement for a specific table",
    "table_stats": "Get row count and data size for tables",
}


@dataclass
class _ColumnInfo:
    name: str
    type: str
    default_kind: str = ""
    default_expression: str = ""
    comment: str = ""


def _get_client():
    return clickhouse_connect.get_client(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_PORT,
        username=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD,
        database=CLICKHOUSE_DATABASE,
    )


def _format_columns(columns: list[dict]) -> str:
    """Format columns into a human-readable string."""
    lines = []
    for c in columns:
        line = f"  {c['name']:30s} {c['type']}"
        if c.get("comment"):
            line += f"  COMMENT '{c['comment']}'"
        lines.append(line)
    return "\n".join(lines)


def _list_tables() -> ToolResult:
    """List all tables in the database with basic info."""
    start = time.monotonic()
    client = _get_client()
    try:
        result = client.query(f"""
            SELECT
                name,
                engine,
                total_rows,
                formatReadableSize(total_bytes) AS size,
                create_table_query
            FROM system.tables
            WHERE database = '{CLICKHOUSE_DATABASE}'
            ORDER BY name
        """)

        tables = []
        for row in result.result_rows:
            tables.append({
                "name": row[0],
                "engine": row[1],
                "total_rows": row[2],
                "size": row[3],
            })

        elapsed = (time.monotonic() - start) * 1000
        record_tool_call("query_metadata", "success", elapsed / 1000)

        summary = f"Found {len(tables)} tables in '{CLICKHOUSE_DATABASE}':\n"
        for t in tables:
            rows = t["total_rows"] if t["total_rows"] else "0"
            summary += f"  - {t['name']}: engine={t['engine']}, rows={rows}, size={t['size']}\n"

        return ToolResult(
            success=True,
            data={"tables": tables, "summary": summary},
            latency_ms=elapsed,
        )
    except Exception as e:
        elapsed = (time.monotonic() - start) * 1000
        record_tool_call("query_metadata", "failed", elapsed / 1000)
        return ToolResult(success=False, data=None, error=str(e), latency_ms=elapsed)
    finally:
        client.close()


def _get_table_schema(table_name: str) -> ToolResult:
    """Get column schema for a specific table."""
    start = time.monotonic()
    client = _get_client()
    try:
        # Check if table exists
        exists = client.command(f"""
            SELECT count() FROM system.tables
            WHERE database = '{CLICKHOUSE_DATABASE}' AND name = '{table_name}'
        """)
        if exists == 0:
            return ToolResult(
                success=False,
                data=None,
                error=f"Table '{table_name}' not found in database '{CLICKHOUSE_DATABASE}'",
                latency_ms=(time.monotonic() - start) * 1000,
            )

        # Get columns
        result = client.query(f"""
            SELECT
                name,
                type,
                default_kind,
                default_expression,
                comment
            FROM system.columns
            WHERE database = '{CLICKHOUSE_DATABASE}' AND table = '{table_name}'
            ORDER BY position
        """)

        columns = []
        for row in result.result_rows:
            columns.append({
                "name": row[0],
                "type": row[1],
                "default_kind": row[2] or "",
                "default_expression": row[3] or "",
                "comment": row[4] or "",
            })

        # Get table engine info
        table_info = client.query(f"""
            SELECT engine, total_rows, formatReadableSize(total_bytes) AS size,
                   sorting_key, partition_key
            FROM system.tables
            WHERE database = '{CLICKHOUSE_DATABASE}' AND name = '{table_name}'
        """).result_rows[0]

        elapsed = (time.monotonic() - start) * 1000
        record_tool_call("query_metadata", "success", elapsed / 1000)

        summary = f"Table '{table_name}' ({table_info[0]}):\n"
        summary += f"  Engine: {table_info[0]}\n"
        summary += f"  Rows: {table_info[1] if table_info[1] else '0'}\n"
        summary += f"  Size: {table_info[2]}\n"
        if table_info[3]:
            summary += f"  Sorting key: {table_info[3]}\n"
        if table_info[4]:
            summary += f"  Partition key: {table_info[4]}\n"
        summary += f"\nColumns ({len(columns)}):\n"
        summary += _format_columns(columns)

        return ToolResult(
            success=True,
            data={"table": table_name, "engine": table_info[0], "columns": columns, "summary": summary},
            latency_ms=elapsed,
        )
    except Exception as e:
        elapsed = (time.monotonic() - start) * 1000
        record_tool_call("query_metadata", "failed", elapsed / 1000)
        return ToolResult(success=False, data=None, error=str(e), latency_ms=elapsed)
    finally:
        client.close()


def _get_table_ddl(table_name: str) -> ToolResult:
    """Get CREATE TABLE DDL for a specific table."""
    start = time.monotonic()
    client = _get_client()
    try:
        ddl = client.command(f"SHOW CREATE TABLE `{table_name}`")

        elapsed = (time.monotonic() - start) * 1000
        record_tool_call("query_metadata", "success", elapsed / 1000)

        summary = f"CREATE TABLE DDL for '{table_name}':\n{ddl}"

        return ToolResult(
            success=True,
            data={"table": table_name, "ddl": ddl, "summary": summary},
            latency_ms=elapsed,
        )
    except Exception as e:
        elapsed = (time.monotonic() - start) * 1000
        record_tool_call("query_metadata", "failed", elapsed / 1000)
        return ToolResult(success=False, data=None, error=str(e), latency_ms=elapsed)
    finally:
        client.close()


def _get_table_stats() -> ToolResult:
    """Get row count and size stats for all tables."""
    start = time.monotonic()
    client = _get_client()
    try:
        # ClickHouse 25.8 renamed data_uncompressed_bytes → total_bytes_uncompressed
        result = client.query(f"""
            SELECT
                name,
                engine,
                total_rows,
                formatReadableSize(total_bytes) AS size,
                formatReadableSize(total_bytes_uncompressed) AS uncompressed_size,
                round(total_bytes_uncompressed / NULLIF(total_bytes, 0), 1) AS compression_ratio
            FROM system.tables
            WHERE database = '{CLICKHOUSE_DATABASE}'
            ORDER BY total_bytes DESC
        """)

        tables = []
        for row in result.result_rows:
            tables.append({
                "name": row[0],
                "engine": row[1],
                "total_rows": row[2],
                "size": row[3],
                "uncompressed_size": row[4],
                "compression_ratio": row[5],
            })

        elapsed = (time.monotonic() - start) * 1000
        record_tool_call("query_metadata", "success", elapsed / 1000)

        summary = f"Table statistics for '{CLICKHOUSE_DATABASE}':\n"
        summary += f"{'Table':<30s} {'Engine':<15s} {'Rows':>12s} {'Size':>10s}\n"
        summary += "-" * 70 + "\n"
        for t in tables:
            rows = str(t["total_rows"]) if t["total_rows"] else "0"
            summary += f"{t['name']:<30s} {t['engine']:<15s} {rows:>12s} {t['size']:>10s}\n"

        return ToolResult(
            success=True,
            data={"tables": tables, "summary": summary},
            latency_ms=elapsed,
        )
    except Exception as e:
        elapsed = (time.monotonic() - start) * 1000
        record_tool_call("query_metadata", "failed", elapsed / 1000)
        return ToolResult(success=False, data=None, error=str(e), latency_ms=elapsed)
    finally:
        client.close()


class QueryMetadataTool(Tool):
    name = "query_metadata"
    description = "Query table metadata from ClickHouse system tables. Use this to discover available tables, check column schemas, or get CREATE TABLE DDL."
    input_schema = {
        "type": "object",
        "properties": {
            "query_type": {
                "type": "string",
                "enum": list(QUERY_TYPES.keys()),
                "description": "Type of metadata query: 'list_tables' (list all tables), 'schema' (columns for a table), 'ddl' (CREATE TABLE statement), 'table_stats' (row counts and sizes)",
            },
            "table_name": {
                "type": "string",
                "description": "Table name (required for 'schema' and 'ddl' query types)",
            },
        },
        "required": ["query_type"],
    }

    async def execute(self, input: dict) -> ToolResult:
        start = time.monotonic()
        query_type = input.get("query_type", "")
        table_name = input.get("table_name", "")

        _log.debug("execute  query_type=%s table=%s", query_type, table_name)

        if query_type not in QUERY_TYPES:
            return ToolResult(
                success=False,
                data=None,
                error=f"Unknown query_type '{query_type}'. Supported: {', '.join(QUERY_TYPES.keys())}",
                latency_ms=(time.monotonic() - start) * 1000,
            )

        if query_type in ("schema", "ddl") and not table_name:
            return ToolResult(
                success=False,
                data=None,
                error=f"table_name is required for query_type '{query_type}'",
                latency_ms=(time.monotonic() - start) * 1000,
            )

        if query_type == "list_tables":
            return _list_tables()
        elif query_type == "schema":
            return _get_table_schema(table_name)
        elif query_type == "ddl":
            return _get_table_ddl(table_name)
        elif query_type == "table_stats":
            return _get_table_stats()

        return ToolResult(success=False, data=None, error="Unknown query type")


# Register the tool
ToolRegistry.register(QueryMetadataTool())
