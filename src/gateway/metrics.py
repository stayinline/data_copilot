from prometheus_client import Counter, Histogram, Gauge, generate_latest, CONTENT_TYPE_LATEST

from fastapi import Request
from fastapi.responses import Response

# Request metrics
copilot_chat_requests_total = Counter(
    "copilot_chat_requests_total",
    "Total chat requests",
    ["status"],
)

copilot_chat_latency_seconds = Histogram(
    "copilot_chat_latency_seconds",
    "Chat request latency",
)

# Tool call metrics
copilot_tool_calls_total = Counter(
    "copilot_tool_calls_total",
    "Total tool calls",
    ["tool_name", "status"],
)

copilot_tool_latency_seconds = Histogram(
    "copilot_tool_latency_seconds",
    "Tool call latency",
    ["tool_name"],
)

# LLM metrics
copilot_llm_latency_seconds = Histogram(
    "copilot_llm_latency_seconds",
    "LLM call latency",
)

# SQL metrics
copilot_sql_success_rate = Gauge(
    "copilot_sql_success_rate",
    "SQL execution success rate",
)

# Cache metrics
copilot_cache_hits_total = Counter(
    "copilot_cache_hits_total",
    "Total cache hits",
    ["cache_type"],
)

copilot_cache_misses_total = Counter(
    "copilot_cache_misses_total",
    "Total cache misses",
    ["cache_type"],
)

# SQL auto-fix metrics
copilot_sql_auto_fix_total = Counter(
    "copilot_sql_auto_fix_total",
    "Total SQL auto-fix attempts",
    ["status"],
)


def record_cache_hit(cache_type: str = "query"):
    """Record a cache hit."""
    copilot_cache_hits_total.labels(cache_type=cache_type).inc()


def record_cache_miss(cache_type: str = "query"):
    """Record a cache miss."""
    copilot_cache_misses_total.labels(cache_type=cache_type).inc()


def record_sql_auto_fix(status: str = "success"):
    """Record SQL auto-fix attempt."""
    copilot_sql_auto_fix_total.labels(status=status).inc()


def record_chat(status: str, latency: float):
    """Record a chat request metric."""
    copilot_chat_requests_total.labels(status=status).inc()
    copilot_chat_latency_seconds.observe(latency)


def record_tool_call(tool_name: str, status: str, latency: float):
    """Record a tool call metric."""
    copilot_tool_calls_total.labels(tool_name=tool_name, status=status).inc()
    copilot_tool_latency_seconds.labels(tool_name=tool_name).observe(latency)


def record_llm_call(latency: float):
    """Record an LLM call metric."""
    copilot_llm_latency_seconds.observe(latency)


def prometheus_metrics_endpoint(request: Request) -> Response:
    """FastAPI handler that returns Prometheus metrics."""
    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)
