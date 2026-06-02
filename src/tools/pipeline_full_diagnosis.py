"""End-to-end pipeline diagnosis tool.

Runs the full cascade (check_flink → check_kafka → check_logs → check_alerts)
in a single tool call, returning an aggregated report so the LLM doesn't need
to orchestrate individual steps.
"""
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

_log = get_logger("tool.pipeline_full_diagnosis")

DIAGNOSIS_STEPS = ["check_flink", "check_kafka", "check_logs", "check_alerts"]


def _esc(val: str) -> str:
    return val.replace("'", "\\'")


def _build_logs_sql(filters: dict) -> str:
    conditions = []
    for key, col in [("pipeline", "pipeline"), ("error_type", "error_type"), ("log_level", "log_level")]:
        if filters.get(key):
            conditions.append(f"{col} = '{_esc(filters[key])}'")
    for key, col, op in [("start_time", "log_time", ">="), ("end_time", "log_time", "<=")]:
        if filters.get(key):
            conditions.append(f"{col} {op} '{_esc(filters[key])}'")
    limit = int(filters.get("limit", 50))
    where = " AND ".join(conditions)
    return (
        "SELECT log_id, pipeline, error_type, log_level, log_time, message "
        f"FROM pipeline_logs {'WHERE ' + where if where else ''} "
        f"ORDER BY log_time DESC LIMIT {limit}"
    )


def _build_alerts_sql(filters: dict) -> str:
    conditions = []
    for key, col in [("service", "service"), ("alert_type", "alert_type"), ("level", "level"), ("status", "status")]:
        if filters.get(key):
            conditions.append(f"{col} = '{_esc(filters[key])}'")
    for key, col, op in [("start_time", "alert_time", ">="), ("end_time", "alert_time", "<=")]:
        if filters.get(key):
            conditions.append(f"{col} {op} '{_esc(filters[key])}'")
    where_clause = " AND ".join(conditions)
    return (
        "SELECT alert_id, service, alert_type, level, alert_time, status "
        f"FROM alerts {'WHERE ' + where_clause if where_clause else ''} "
        "ORDER BY alert_time DESC LIMIT 100"
    )


SQL_MAP = {
    "check_flink": "SELECT job_id, status, start_time, checkpoint_delay_s, backpressure, last_checkpoint_status, last_checkpoint_time, throughput FROM flink_jobs ORDER BY job_id LIMIT 100",
    "check_kafka": "SELECT topic, partition_count, producer_tps, consumer_group, total_lag, consumer_status, consumer_tps, max_partition_lag, check_date FROM kafka_topics ORDER BY total_lag DESC LIMIT 100",
}


def _convert(v):
    if isinstance(v, datetime):
        return v.strftime("%Y-%m-%d %H:%M:%S")
    if isinstance(v, date):
        return v.strftime("%Y-%m-%d")
    if isinstance(v, Decimal):
        return float(v)
    return v


def _run_query(query: str, columns: list[str]) -> list[dict]:
    client = clickhouse_connect.get_client(
        host=CLICKHOUSE_HOST, port=CLICKHOUSE_PORT,
        username=CLICKHOUSE_USER, password=CLICKHOUSE_PASSWORD,
        database=CLICKHOUSE_DATABASE,
    )
    try:
        result = client.query(query)
        return [
            {col: _convert(v) for col, v in zip(result.column_names, row)}
            for row in result.result_rows
        ]
    finally:
        client.close()


def _analyze_flink(rows: list[dict]) -> dict:
    """Summarize Flink job health."""
    if not rows:
        return {"status": "no_data", "summary": "未查询到 Flink 任务数据"}
    total = len(rows)
    failed = [r for r in rows if r.get("status") not in ("RUNNING", "RUNNING", "running")]
    backpressure_high = [r for r in rows if str(r.get("backpressure", "")).upper() in ("HIGH", "CRITICAL")]
    delayed = [r for r in rows if (r.get("checkpoint_delay_s") or 0) > 60]
    summary = f"共 {total} 个任务"
    issues = []
    if failed:
        issues.append(f"{len(failed)} 个任务状态非 RUNNING")
    if backpressure_high:
        issues.append(f"{len(backpressure_high)} 个任务存在高反压")
    if delayed:
        issues.append(f"{len(delayed)} 个任务 Checkpoint 延迟 > 60s")
    if issues:
        summary += "，" + "；".join(issues)
    else:
        summary += "，所有任务运行正常"
    return {"status": "ok" if not issues else "warning", "summary": summary, "issues": issues, "total_jobs": total}


def _analyze_kafka(rows: list[dict]) -> dict:
    """Summarize Kafka consumer health."""
    if not rows:
        return {"status": "no_data", "summary": "未查询到 Kafka 主题数据"}
    total = len(rows)
    lagging = [r for r in rows if (r.get("total_lag") or 0) > 10000]
    max_lag = max((r.get("total_lag") or 0) for r in rows) if rows else 0
    summary = f"共 {total} 个主题"
    issues = []
    if lagging:
        issues.append(f"{len(lagging)} 个主题消费延迟 > 10000")
    if max_lag > 100000:
        issues.append(f"最大延迟 {max_lag}")
    if issues:
        summary += "，" + "；".join(issues)
    else:
        summary += "，消费正常"
    return {"status": "ok" if not issues else "warning", "summary": summary, "issues": issues, "max_lag": max_lag}


def _analyze_logs(rows: list[dict]) -> dict:
    """Summarize pipeline logs."""
    if not rows:
        return {"status": "no_data", "summary": "未查询到日志数据"}
    total = len(rows)
    errors = [r for r in rows if str(r.get("log_level", "")).upper() == "ERROR"]
    error_types = {}
    for r in errors:
        et = r.get("error_type", "unknown")
        error_types[et] = error_types.get(et, 0) + 1
    summary = f"共 {total} 条日志，其中 ERROR {len(errors)} 条"
    if error_types:
        top = sorted(error_types.items(), key=lambda x: x[1], reverse=True)[:3]
        summary += "，主要错误类型：" + "、".join(f"{k}({v}次)" for k, v in top)
    return {"status": "ok" if not errors else "warning", "summary": summary, "error_count": len(errors), "top_errors": error_types}


def _analyze_alerts(rows: list[dict]) -> dict:
    """Summarize alerts."""
    if not rows:
        return {"status": "no_data", "summary": "未查询到告警记录"}
    total = len(rows)
    open_alerts = [r for r in rows if str(r.get("status", "")).lower() == "open"]
    critical = [r for r in rows if str(r.get("level", "")).upper() in ("CRITICAL", "FATAL")]
    summary = f"共 {total} 条告警"
    issues = []
    if open_alerts:
        issues.append(f"{len(open_alerts)} 条未关闭")
    if critical:
        issues.append(f"{len(critical)} 条 CRITICAL 级别")
    if issues:
        summary += "，" + "；".join(issues)
    else:
        summary += "，均为已处理或低级别"
    return {"status": "ok" if not open_alerts and not critical else "warning", "summary": summary, "open_count": len(open_alerts), "critical_count": len(critical)}


class PipelineFullDiagnosisTool(Tool):
    name = "pipeline_full_diagnosis"
    description = (
        "端到端管道诊断：自动依次检查 Flink 任务状态、Kafka 消费延迟、"
        "Pipeline 错误日志和告警记录，返回汇总报告。"
        "用于快速定位 Pipeline 故障根因，一次调用完成全链路排查。"
    )
    permission_tag = "pipeline:read"
    timeout = 60
    input_schema = {
        "type": "object",
        "properties": {
            "filters": {
                "type": "object",
                "description": "可选过滤条件，适用于日志和告警查询",
                "properties": {
                    "pipeline": {"type": "string", "description": "Pipeline 名称"},
                    "error_type": {"type": "string"},
                    "log_level": {"type": "string"},
                    "service": {"type": "string"},
                    "start_time": {"type": "string", "description": "开始时间 YYYY-MM-DD HH:MM:SS"},
                    "end_time": {"type": "string", "description": "结束时间 YYYY-MM-DD HH:MM:SS"},
                    "limit": {"type": "integer", "minimum": 1, "maximum": 500, "description": "日志查询条数限制，默认 50"},
                },
            },
        },
        "required": [],
    }

    async def execute(self, input: dict) -> ToolResult:
        start = time.monotonic()
        filters = input.get("filters") or {}
        _log.debug("execute  filters=%s", filters)

        step_results = {}
        overall_issues = []
        has_critical = False

        # Step 1: check_flink
        try:
            flink_rows = _run_query(SQL_MAP["check_flink"], ["job_id", "status", "start_time", "checkpoint_delay_s", "backpressure", "last_checkpoint_status", "last_checkpoint_time", "throughput"])
            analysis = _analyze_flink(flink_rows)
            step_results["check_flink"] = {"analysis": analysis, "raw_rows": len(flink_rows)}
            if analysis["status"] == "warning":
                overall_issues.append(f"Flink: {analysis['summary']}")
                if any("高反压" in i or "非 RUNNING" in i for i in analysis.get("issues", [])):
                    has_critical = True
        except Exception as e:
            step_results["check_flink"] = {"error": str(e)}
            overall_issues.append(f"Flink 检查失败: {e}")

        # Step 2: check_kafka
        try:
            kafka_rows = _run_query(SQL_MAP["check_kafka"], ["topic", "partition_count", "producer_tps", "consumer_group", "total_lag", "consumer_status", "consumer_tps", "max_partition_lag", "check_date"])
            analysis = _analyze_kafka(kafka_rows)
            step_results["check_kafka"] = {"analysis": analysis, "raw_rows": len(kafka_rows)}
            if analysis["status"] == "warning":
                overall_issues.append(f"Kafka: {analysis['summary']}")
                if analysis.get("max_lag", 0) > 100000:
                    has_critical = True
        except Exception as e:
            step_results["check_kafka"] = {"error": str(e)}
            overall_issues.append(f"Kafka 检查失败: {e}")

        # Step 3: check_logs
        try:
            logs_sql = _build_logs_sql(filters)
            logs_rows = _run_query(logs_sql, ["log_id", "pipeline", "error_type", "log_level", "log_time", "message"])
            analysis = _analyze_logs(logs_rows)
            step_results["check_logs"] = {"analysis": analysis, "raw_rows": len(logs_rows)}
            if analysis["status"] == "warning":
                overall_issues.append(f"日志: {analysis['summary']}")
        except Exception as e:
            step_results["check_logs"] = {"error": str(e)}
            overall_issues.append(f"日志查询失败: {e}")

        # Step 4: check_alerts
        try:
            alerts_sql = _build_alerts_sql(filters)
            alerts_rows = _run_query(alerts_sql, ["alert_id", "service", "alert_type", "level", "alert_time", "status"])
            analysis = _analyze_alerts(alerts_rows)
            step_results["check_alerts"] = {"analysis": analysis, "raw_rows": len(alerts_rows)}
            if analysis["status"] == "warning":
                overall_issues.append(f"告警: {analysis['summary']}")
                if analysis.get("critical_count", 0) > 0:
                    has_critical = True
        except Exception as e:
            step_results["check_alerts"] = {"error": str(e)}
            overall_issues.append(f"告警查询失败: {e}")

        # Build overall summary
        if overall_issues:
            diagnosis_summary = f"发现 {len(overall_issues)} 项异常：\n" + "\n".join(f"- {i}" for i in overall_issues)
            if has_critical:
                diagnosis_summary = "【严重】" + diagnosis_summary
        else:
            diagnosis_summary = "全链路检查完成，未发现异常。Flink 任务运行正常，Kafka 消费无延迟，无 ERROR 级别日志，无未关闭告警。"

        data = {
            "diagnosis_summary": diagnosis_summary,
            "has_critical": has_critical,
            "issue_count": len(overall_issues),
            "steps": step_results,
        }

        elapsed = (time.monotonic() - start) * 1000
        _log.debug("success  issues=%d elapsed=%.0fms", len(overall_issues), elapsed)
        return ToolResult(success=True, data=data, latency_ms=elapsed)


# Register the tool
ToolRegistry.register(PipelineFullDiagnosisTool())
