import time
from datetime import date, datetime
from decimal import Decimal

import clickhouse_connect

from src.tools.base import Tool, ToolResult, ToolRegistry
from src.utils.logging import get_logger
from config import (
    CLICKHOUSE_HOST,
    CLICKHOUSE_PORT,
    CLICKHOUSE_USER,
    CLICKHOUSE_PASSWORD,
    CLICKHOUSE_DATABASE,
)

_log = get_logger("tool.pipeline_troubleshoot")

OPERATIONS = {
    "check_flink": {
        "description": "Check Flink job status, checkpoint delays, backpressure, and throughput.",
        "sql": "SELECT job_id, status, start_time, checkpoint_delay_s, backpressure, last_checkpoint_status, last_checkpoint_time, throughput FROM flink_jobs ORDER BY job_id LIMIT 100",
        "format": "list_flink_jobs",
    },
    "check_kafka": {
        "description": "Check Kafka topic lag, consumer group status, and throughput.",
        "sql": "SELECT topic, partition_count, producer_tps, consumer_group, total_lag, consumer_status, consumer_tps, max_partition_lag, check_date FROM kafka_topics ORDER BY total_lag DESC LIMIT 100",
        "format": "list_kafka_topics",
    },
    "check_logs": {
        "description": "Query pipeline error/warning logs. Supports filters: pipeline (str), error_type (str), log_level (str: ERROR/WARN/INFO), start_time (str: YYYY-MM-DD HH:MM:SS), end_time (str: YYYY-MM-DD HH:MM:SS), limit (int).",
        "sql": None,  # Dynamic SQL built in execute
        "format": "list_logs",
    },
    "check_alerts": {
        "description": "Check alert records. Supports filters: service (str), alert_type (str), level (str: CRITICAL/WARN/INFO), status (str: open/resolved), start_time (str: YYYY-MM-DD HH:MM:SS), end_time (str: YYYY-MM-DD HH:MM:SS).",
        "sql": None,  # Dynamic SQL built in execute
        "format": "list_alerts",
    },
}


class PipelineTroubleshootTool(Tool):
    name = "pipeline_troubleshoot"
    description = (
        "Troubleshoot data pipeline issues. Supports operations: "
        "check_flink (Flink job status), "
        "check_kafka (Kafka topic lag/consumer group), "
        "check_logs (pipeline error logs), "
        "check_alerts (alert records). "
        "Use this to diagnose pipeline failures, Kafka lag, Flink backpressure, "
        "and application log errors."
    )
    input_schema = {
        "type": "object",
        "properties": {
            "operation": {
                "type": "string",
                "enum": ["check_flink", "check_kafka", "check_logs", "check_alerts"],
                "description": "Troubleshooting operation to run",
            },
            "filters": {
                "type": "object",
                "description": "Optional filters. Varies by operation. See check_logs and check_alerts for available filter keys.",
                "properties": {
                    "pipeline": {"type": "string"},
                    "error_type": {"type": "string"},
                    "log_level": {"type": "string"},
                    "service": {"type": "string"},
                    "alert_type": {"type": "string"},
                    "level": {"type": "string"},
                    "status": {"type": "string"},
                    "start_time": {"type": "string"},
                    "end_time": {"type": "string"},
                    "limit": {"type": "integer"},
                },
            },
        },
        "required": ["operation"],
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
        operation = input.get("operation")
        filters = input.get("filters") or {}

        if operation not in OPERATIONS:
            elapsed = (time.monotonic() - start) * 1000
            return ToolResult(
                success=False,
                data=None,
                error=f"Unknown operation: {operation}. Available: {', '.join(OPERATIONS.keys())}",
                latency_ms=elapsed,
            )

        op = OPERATIONS[operation]
        _log.debug("execute  operation=%s", operation)

        try:
            # Build SQL
            if op["sql"]:
                query = op["sql"]
            else:
                query = self._build_dynamic_sql(operation, filters)

            _log.debug("running query: %s", query)
            client = self._get_client()
            result = client.query(query)

            def _convert(v):
                if isinstance(v, datetime):
                    return v.strftime("%Y-%m-%d %H:%M:%S")
                if isinstance(v, date):
                    return v.strftime("%Y-%m-%d")
                if isinstance(v, Decimal):
                    return float(v)
                return v

            rows = [
                {col: _convert(v) for col, v in zip(result.column_names, row)}
                for row in result.result_rows
            ]
            client.close()

            data = {
                "operation": operation,
                "columns": result.column_names,
                "rows": rows,
                "row_count": len(rows),
            }

            elapsed = (time.monotonic() - start) * 1000
            _log.debug("success  operation=%s rows=%d", operation, len(rows))
            return ToolResult(success=True, data=data, latency_ms=elapsed)

        except Exception as e:
            _log.debug("failed  operation=%s error=%s", operation, str(e)[:200])
            elapsed = (time.monotonic() - start) * 1000
            return ToolResult(
                success=False, data=None, error=str(e), latency_ms=elapsed
            )

    @staticmethod
    def _build_dynamic_sql(operation: str, filters: dict) -> str:
        if operation == "check_logs":
            return PipelineTroubleshootTool._build_logs_sql(filters)
        elif operation == "check_alerts":
            return PipelineTroubleshootTool._build_alerts_sql(filters)
        return ""

    @staticmethod
    def _esc(val: str) -> str:
        """Escape a string value for SQL literal."""
        return val.replace("'", "\\'")

    @staticmethod
    def _build_logs_sql(filters: dict) -> str:
        conditions = []

        pipeline = filters.get("pipeline")
        if pipeline:
            conditions.append(f"pipeline = '{PipelineTroubleshootTool._esc(pipeline)}'")

        error_type = filters.get("error_type")
        if error_type:
            conditions.append(f"error_type = '{PipelineTroubleshootTool._esc(error_type)}'")

        log_level = filters.get("log_level")
        if log_level:
            conditions.append(f"log_level = '{PipelineTroubleshootTool._esc(log_level)}'")

        start_time = filters.get("start_time")
        if start_time:
            conditions.append(f"log_time >= '{PipelineTroubleshootTool._esc(start_time)}'")

        end_time = filters.get("end_time")
        if end_time:
            conditions.append(f"log_time <= '{PipelineTroubleshootTool._esc(end_time)}'")

        limit = int(filters.get("limit", 50))

        where = " AND ".join(conditions)
        return (
            "SELECT log_id, pipeline, error_type, log_level, log_time, message "
            f"FROM pipeline_logs {'WHERE ' + where if where else ''} "
            f"ORDER BY log_time DESC LIMIT {limit}"
        )

    @staticmethod
    def _build_alerts_sql(filters: dict) -> str:
        conditions = []

        service = filters.get("service")
        if service:
            conditions.append(f"service = '{PipelineTroubleshootTool._esc(service)}'")

        alert_type = filters.get("alert_type")
        if alert_type:
            conditions.append(f"alert_type = '{PipelineTroubleshootTool._esc(alert_type)}'")

        level = filters.get("level")
        if level:
            conditions.append(f"level = '{PipelineTroubleshootTool._esc(level)}'")

        status = filters.get("status")
        if status:
            conditions.append(f"status = '{PipelineTroubleshootTool._esc(status)}'")

        start_time = filters.get("start_time")
        if start_time:
            conditions.append(f"alert_time >= '{PipelineTroubleshootTool._esc(start_time)}'")

        end_time = filters.get("end_time")
        if end_time:
            conditions.append(f"alert_time <= '{PipelineTroubleshootTool._esc(end_time)}'")

        where = " AND ".join(conditions)
        return (
            "SELECT alert_id, service, alert_type, level, alert_time, status "
            f"FROM alerts {'WHERE ' + where if where else ''} "
            "ORDER BY alert_time DESC LIMIT 100"
        )


# Register the tool
ToolRegistry.register(PipelineTroubleshootTool())
