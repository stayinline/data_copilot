"""
Root Cause Analysis tool — automatically drills down metric anomalies
to find the root cause by region and category dimensions.

Multi-step analysis:
1. Overall metric comparison (today vs yesterday)
2. Region-level breakdown to find the worst performer
3. Category-level breakdown within the worst region
4. Orders table cross-validation
5. Structured result returned to LLM for natural language report
"""

import time
from datetime import date, datetime, timedelta
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

_log = get_logger("tool.root_cause_analysis")


def _get_client():
    return clickhouse_connect.get_client(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_PORT,
        username=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD,
        database=CLICKHOUSE_DATABASE,
    )


def _convert(v):
    if isinstance(v, datetime):
        return v.strftime("%Y-%m-%d %H:%M:%S")
    if isinstance(v, date):
        return v.strftime("%Y-%m-%d")
    if isinstance(v, Decimal):
        return float(v)
    return v


def _scalar(client, sql):
    r = client.query(sql)
    if not r.result_rows:
        return None
    val = r.result_rows[0][0]
    return _convert(val) if val is not None else None


def _rows(client, sql):
    r = client.query(sql)
    return [
        {col: _convert(val) for col, val in zip(r.column_names, row)}
        for row in r.result_rows
    ]


class RootCauseAnalysisTool(Tool):
    name = "root_cause_analysis"
    description = (
        "Automatically analyze why a business metric dropped. "
        "Given a metric name (e.g., GMV, DAU, 新客数, 转化率, 客单价), "
        "it compares today vs yesterday, drills down by region and category, "
        "and cross-validates with order data. Returns a structured analysis "
        "with the root cause identified. Use this when users ask '为什么XX下降了' "
        "or similar anomaly investigation questions."
    )
    permission_tag = "analysis:read"
    timeout = 45
    input_schema = {
        "type": "object",
        "properties": {
            "metric": {
                "type": "string",
                "description": "Metric name to analyze. One of: GMV, DAU, 新客数, 转化率, 客单价",
            },
            "target_date": {
                "type": "string",
                "description": "Date to analyze (YYYY-MM-DD). Defaults to today.",
            },
        },
        "required": ["metric"],
    }

    async def execute(self, input: dict) -> ToolResult:
        start = time.monotonic()
        metric = input.get("metric", "GMV")
        target_date_str = input.get("target_date")

        client = _get_client()
        try:
            result = self._run_analysis(client, metric, target_date_str)
            elapsed = (time.monotonic() - start) * 1000
            _log.debug("RCA done in %.0fms  metric=%s", elapsed, metric)
            return ToolResult(success=True, data=result, latency_ms=elapsed)
        except Exception as e:
            elapsed = (time.monotonic() - start) * 1000
            _log.error("RCA failed: %s", str(e)[:200])
            return ToolResult(success=False, data=None, error=str(e), latency_ms=elapsed)
        finally:
            client.close()

    def _run_analysis(self, client, metric: str, target_date_str: str | None) -> dict:
        today = date.today() if not target_date_str else date.fromisoformat(target_date_str)
        yesterday = today - timedelta(days=1)
        today_str = str(today)
        yesterday_str = str(yesterday)
        table = "metrics" if metric else "metrics"

        # ── Step 1: Overall comparison (today vs yesterday, all regions) ──
        overall_today = _scalar(
            client,
            f"SELECT sum(metric_value) FROM {table} "
            f"WHERE metric_date='{today_str}' AND metric_name='{metric}' AND category IN ('', '合计')",
        )
        overall_yesterday = _scalar(
            client,
            f"SELECT sum(metric_value) FROM {table} "
            f"WHERE metric_date='{yesterday_str}' AND metric_name='{metric}' AND category IN ('', '合计')",
        )
        overall_drop_pct = (
            round((1 - float(overall_today) / float(overall_yesterday)) * 100, 1)
            if overall_yesterday and overall_today
            else 0
        )

        # ── Step 2: Region breakdown ──
        region_rows = _rows(
            client,
            f"SELECT region, sum(metric_value) as value FROM {table} "
            f"WHERE metric_date='{today_str}' AND metric_name='{metric}' "
            f"AND category IN ('', '合计') GROUP BY region ORDER BY value DESC",
        )
        region_yesterday = _rows(
            client,
            f"SELECT region, sum(metric_value) as value FROM {table} "
            f"WHERE metric_date='{yesterday_str}' AND metric_name='{metric}' "
            f"AND category IN ('', '合计') GROUP BY region ORDER BY value DESC",
        )
        yesterday_map = {r["region"]: float(r["value"]) for r in region_yesterday}

        # Find the worst dropping region
        worst_region = None
        worst_drop_pct = 0
        for r in region_rows:
            region = r["region"]
            today_val = float(r["value"])
            y_val = yesterday_map.get(region, 0)
            drop = (1 - today_val / y_val) * 100 if y_val > 0 else 0
            r["yesterday_value"] = y_val
            r["drop_pct"] = round(drop, 1)
            if drop > worst_drop_pct:
                worst_drop_pct = drop
                worst_region = region

        # ── Step 3: Category breakdown within the worst region ──
        category_rows = []
        worst_region_category_drop_pct = 0
        worst_category = None
        if worst_region:
            category_rows = _rows(
                client,
                f"SELECT category, sum(metric_value) as value FROM {table} "
                f"WHERE metric_date='{today_str}' AND metric_name='{metric}' "
                f"AND region='{worst_region}' AND category NOT IN ('', '合计') "
                f"GROUP BY category ORDER BY value DESC",
            )
            category_yesterday = _rows(
                client,
                f"SELECT category, sum(metric_value) as value FROM {table} "
                f"WHERE metric_date='{yesterday_str}' AND metric_name='{metric}' "
                f"AND region='{worst_region}' AND category NOT IN ('', '合计') "
                f"GROUP BY category ORDER BY value DESC",
            )
            cat_yesterday_map = {r["category"]: float(r["value"]) for r in category_yesterday}

            for r in category_rows:
                cat = r["category"]
                today_val = float(r["value"])
                y_val = cat_yesterday_map.get(cat, 0)
                drop = (1 - today_val / y_val) * 100 if y_val > 0 else 0
                r["yesterday_value"] = y_val
                r["drop_pct"] = round(drop, 1)
                # Contribution: how much of the region's drop does this category account for?
                region_today_total = sum(float(c["value"]) for c in category_rows)
                region_yesterday_total = sum(cat_yesterday_map.get(c["category"], 0) for c in category_rows)
                region_drop_amt = region_yesterday_total - region_today_total
                cat_drop_amt = y_val - today_val
                r["contribution_pct"] = round(cat_drop_amt / region_drop_amt * 100, 1) if region_drop_amt > 0 else 0
                if drop > worst_region_category_drop_pct:
                    worst_region_category_drop_pct = drop
                    worst_category = cat

        # ── Step 4: Cross-validate with orders table ──
        order_validation = None
        if worst_region and worst_category:
            # Compare today vs yesterday order counts for worst_region + worst_category
            today_orders = _scalar(
                client,
                f"SELECT count() FROM orders WHERE order_date='{today_str}' "
                f"AND region='{worst_region}' AND category='{worst_category}'",
            )
            yesterday_orders = _scalar(
                client,
                f"SELECT count() FROM orders WHERE order_date='{yesterday_str}' "
                f"AND region='{worst_region}' AND category='{worst_category}'",
            )
            today_amount = _scalar(
                client,
                f"SELECT sum(amount) FROM orders WHERE order_date='{today_str}' "
                f"AND region='{worst_region}' AND category='{worst_category}'",
            )
            yesterday_amount = _scalar(
                client,
                f"SELECT sum(amount) FROM orders WHERE order_date='{yesterday_str}' "
                f"AND region='{worst_region}' AND category='{worst_category}'",
            )
            order_drop_pct = (
                round((1 - float(today_orders) / float(yesterday_orders)) * 100, 1)
                if yesterday_orders and today_orders
                else (100 if yesterday_orders else 0)
            )
            order_validation = {
                "region": worst_region,
                "category": worst_category,
                "today_order_count": int(today_orders or 0),
                "yesterday_order_count": int(yesterday_orders or 0),
                "order_drop_pct": order_drop_pct,
                "today_total_amount": float(today_amount or 0),
                "yesterday_total_amount": float(yesterday_amount or 0),
                "amount_drop_pct": (
                    round((1 - float(today_amount) / float(yesterday_amount)) * 100, 1)
                    if yesterday_amount and today_amount
                    else 0
                ),
            }

        # ── Step 5: Assemble result ──
        return {
            "metric": metric,
            "date_range": {"today": today_str, "yesterday": yesterday_str},
            "overall": {
                "today": overall_today if overall_today else 0,
                "yesterday": overall_yesterday if overall_yesterday else 0,
                "drop_pct": overall_drop_pct,
            },
            "region_breakdown": region_rows,
            "worst_region": {
                "name": worst_region,
                "drop_pct": round(worst_drop_pct, 1),
            } if worst_region else None,
            "category_breakdown": category_rows,
            "worst_category": {
                "name": worst_category,
                "drop_pct": round(worst_region_category_drop_pct, 1),
            } if worst_category else None,
            "order_validation": order_validation,
        }


# Register the tool
ToolRegistry.register(RootCauseAnalysisTool())
