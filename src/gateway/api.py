import asyncio
import json
import uuid
from contextlib import asynccontextmanager
from datetime import datetime
import os

from fastapi import FastAPI, Depends, HTTPException, Header, Query, Request
from fastapi.responses import StreamingResponse, Response, FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from sqlalchemy import select, delete
from sqlalchemy.ext.asyncio import AsyncSession

from src.storage.db import get_db, init_db, ChatSession, ToolLog
from src.storage.memory import SessionMemory
from src.gateway.auth import validate_jwt, extract_user_id
from src.gateway.rate_limiter import is_rate_limited
from src.gateway.metrics import record_chat, prometheus_metrics_endpoint
from src.agent.intent import IntentClassifier
from src.agent.context import ExecutionContext
from src.agent.llm_client import chat_completion
from src.agent.summarizer import generate_summary
from src.sql.schema_loader import SCHEMA_TEXT
from src.utils.logging import get_logger
from config import PLANNER_MODE, PLANNER_MAX_TOOL_CALLS, SUMMARY_AFTER_ROUNDS

# Global per-request timeout: 120s for the entire chat flow
# Increased from 60s to accommodate parallel subtask LLM calls and ToT beam search
CHAT_GLOBAL_TIMEOUT = 120

_log = get_logger("gateway.api")


def _extract_analysis_context(tool_results: list[dict]) -> dict | None:
    """Extract structured analysis context from tool results (RCA metrics, worst region/category, etc.)."""
    if not tool_results:
        return None
    context = {}
    for tr in tool_results:
        tool = tr.get("tool", "")
        output = tr.get("output", "")
        if tool == "root_cause_analysis" and output:
            try:
                data = json.loads(output) if isinstance(output, str) else output
                # Extract key RCA findings
                if isinstance(data, dict):
                    if "metric" in data:
                        context["分析指标"] = data["metric"]
                    if "top_dimension" in data:
                        context["最主要维度"] = data["top_dimension"]
                    if "worst_value" in data:
                        context["最差取值"] = data["worst_value"]
                    if "drop_ratio" in data:
                        context["下降比例"] = data["drop_ratio"]
                    # Store the full result as fallback
                    context["rca_raw"] = json.dumps(data, ensure_ascii=False)
            except (json.JSONDecodeError, TypeError):
                context["rca_raw"] = str(output)
        elif tool == "run_sql" and output:
            # Keep last few SQL results for context
            if "last_sql_results" not in context:
                context["last_sql_results"] = []
            if len(context["last_sql_results"]) < 3:
                context["last_sql_results"].append(output[:300])
    return context if context else None


class ChatRequest(BaseModel):
    session_id: str
    user_id: str
    message: str


@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_db()
    import src.tools  # noqa: F401
    yield


app = FastAPI(title="AI Data Copilot", version="0.1.0", lifespan=lifespan)

_static_dir = os.path.join(os.path.dirname(__file__), "..", "..", "static")
if os.path.isdir(_static_dir):
    app.mount("/static", StaticFiles(directory=_static_dir), name="static")


@app.get("/")
async def index():
    return FileResponse(os.path.join(_static_dir, "index.html"))


def _extract_token(authorization: str | None = Header(None)) -> str:
    if not authorization:
        raise HTTPException(status_code=401, detail="Missing Authorization header")
    parts = authorization.split()
    if len(parts) != 2 or parts[0].lower() != "bearer":
        raise HTTPException(status_code=401, detail="Invalid Authorization format")
    return parts[1]


@app.post("/api/v1/chat")
async def chat(
    req: ChatRequest,
    token: str = Depends(_extract_token),
    db: AsyncSession = Depends(get_db),
):
    start = datetime.utcnow()
    _log.debug("POST /api/v1/chat  message=%s  planner_mode=%s", req.message, PLANNER_MODE)

    user_id = extract_user_id(token) or req.user_id

    if is_rate_limited(req.user_id):
        _log.warning("Rate limited  user_id=%s", req.user_id)
        raise HTTPException(status_code=429, detail="Rate limit exceeded")

    _log.debug("Intent classification...")
    intent, intent_confidence = await IntentClassifier.classify_with_confidence(req.message)
    _log.debug("Intent=%s  confidence=%s", intent, intent_confidence)

    session = await db.get(ChatSession, req.session_id)
    if not session:
        session = ChatSession(session_id=req.session_id, user_id=req.user_id, intent_type=intent)
        db.add(session)
    else:
        session.intent_type = intent
        session.updated_at = datetime.utcnow()
    await db.commit()

    trace_id = str(uuid.uuid4())
    _log.debug("trace_id=%s", trace_id)

    _log.debug("Loading history...")
    history = await SessionMemory.load_full_context(req.session_id, req.user_id)
    _log.debug("Loaded %d messages", len(history))

    ctx = ExecutionContext(session_id=req.session_id, user_id=req.user_id)
    ctx.conversation_history = history
    ctx.prior_tool_results = await SessionMemory.load_tool_results(req.session_id, req.user_id)
    ctx.prior_analysis = await SessionMemory.load_analysis_context(req.session_id, req.user_id)

    # Wrap the core chat logic with a global timeout to prevent runaway requests
    async def _run_core_chat() -> str:
        answer = ""

        if intent == "DIRECT":
            _SYSTEM_CHAT_PROMPT = (
                "你是 AI Data Copilot，一个面向数据工程师的多角色智能助手，具备以下能力：\n"
                "1. 数据查询 — 通过自然语言自动生成 SQL 查询并获取数据\n"
                "2. 数据分析 — 指标对比、趋势分析、异常检测与归因分析\n"
                "3. 故障排查 — 数据 Pipeline 排障，包括 Kafka、Flink、日志查询等\n"
                "4. 执行决策 — 多步任务规划与自动化执行\n\n"
                "注意：当前模式下无法直接连接数据库执行 SQL。如果用户提供 SQL 请求审查或报错分析，请基于 SQL 语法规范进行静态分析，指出可能的语法错误、逻辑问题或优化建议。\n"
                "严禁编造任何查询结果或数据。\n\n"
                "可用数据表结构（编写 SQL 时必须严格使用以下表名和字段名，不得虚构）：\n"
                f"{SCHEMA_TEXT}\n\n"
                "请用专业清晰的方式回答，适当展开说明。如果用户的问题与上述能力无关，友好地引导回到数据相关话题。"
            )
            messages = [{"role": "system", "content": _SYSTEM_CHAT_PROMPT}, {"role": "user", "content": req.message}]
            if history:
                messages = [{"role": "system", "content": _SYSTEM_CHAT_PROMPT}] + history + [{"role": "user", "content": req.message}]
            answer = await chat_completion(messages, temperature=0.7)
            if not intent_confidence:
                answer += "\n\n💡 **提问可能不够明确，参考以下示例重新提问：**\n"
                for label, q in IntentClassifier.GUIDANCE_EXAMPLES:
                    answer += f"- {label}：{q}\n"
            _log.debug("DIRECT reply  answer_len=%d", len(answer))
        else:
            _log.debug("Running agent loop  intent=%s  mode=%s", intent, PLANNER_MODE)
            if PLANNER_MODE == "langgraph":
                from src.agent.planner_run import planner_run
                answer = await planner_run(ctx, req.message)
            else:
                from src.agent.react import react_run
                answer = await react_run(ctx, req.message)
            if not intent_confidence:
                answer += "\n\n💡 **提问可能不够明确，参考以下示例重新提问：**\n"
                for label, q in IntentClassifier.GUIDANCE_EXAMPLES:
                    answer += f"- {label}：{q}\n"
            _log.debug("Agent done  answer_len=%d", len(answer))

        # Save tool results and analysis context for next round
        await SessionMemory.save_tool_results(req.session_id, req.user_id, ctx.step_results)
        analysis_ctx = _extract_analysis_context(ctx.step_results)
        if analysis_ctx:
            await SessionMemory.save_analysis_context(req.session_id, req.user_id, analysis_ctx)

        for step in ctx.step_results:
            log = ToolLog(
                session_id=req.session_id,
                trace_id=trace_id,
                tool_name=step["tool"],
                input_params=step["input"],
                output_data=step["output"],
                status="success" if step["success"] else "failed",
                latency_ms=step.get("latency_ms"),
                retry_count=ctx.retry_count,
            )
            db.add(log)
        await db.commit()

        await SessionMemory.save_message(req.session_id, req.user_id, "user", req.message)
        await SessionMemory.save_message(req.session_id, req.user_id, "assistant", answer)

        round_count = await SessionMemory.get_round_count(req.session_id, req.user_id)
        if round_count >= SUMMARY_AFTER_ROUNDS:
            full_history = await SessionMemory.load_history(req.session_id, req.user_id)
            summary = await generate_summary(full_history)
            await SessionMemory.save_summary(req.session_id, req.user_id, summary)
            session.context_summary = summary
            await db.commit()

        _log.debug("answer  content=%s", answer if answer else "")
        elapsed = (datetime.utcnow() - start).total_seconds()
        record_chat("success", elapsed)
        return answer

    try:
        answer = await asyncio.wait_for(_run_core_chat(), timeout=CHAT_GLOBAL_TIMEOUT)
    except asyncio.TimeoutError:
        _log.error("Chat request timed out after %ds  message=%s", CHAT_GLOBAL_TIMEOUT, req.message)
        record_chat("timeout", CHAT_GLOBAL_TIMEOUT)
        raise HTTPException(status_code=504, detail=f"Request timed out after {CHAT_GLOBAL_TIMEOUT}s")

    return {"session_id": req.session_id, "intent": intent, "answer": answer, "status": ctx.state}


async def _sse_generator(ctx: ExecutionContext, message: str, db: AsyncSession, req: ChatRequest, trace_id: str, start_time: datetime, intent: str, intent_confidence: bool = True):
    """Generate SSE events from the streaming agent loop."""
    _log.debug("SSE start  message=%s  mode=%s  intent=%s  confidence=%s", message, PLANNER_MODE, intent, intent_confidence)
    final_answer = None

    # Collect performance metrics
    llm_usage = {
        "llm_calls": 0,
        "prompt_tokens": 0,
        "completion_tokens": 0,
        "total_tokens": 0,
        "llm_latency_ms": 0,
    }
    cache_hits = 0
    cache_misses = 0

    # Emit intent classification event for frontend decision chain visualization
    INTENT_LABELS = {
        "DIRECT": "直接对话",
        "TOOL": "工具调用",
        "TROUBLESHOOT": "故障排查",
    }
    yield f"event: intent\ndata: {json.dumps({'intent': intent, 'label': INTENT_LABELS.get(intent, intent), 'mode': 'langgraph' if PLANNER_MODE == 'langgraph' else 'react'})}\n\n"

    try:
        if intent == "DIRECT":
            # Direct LLM reply -- skip the agent loop
            _SYSTEM_CHAT_PROMPT = (
                "你是 AI Data Copilot，一个面向数据工程师的多角色智能助手，具备以下能力：\n"
                "1. 数据查询 — 通过自然语言自动生成 SQL 查询并获取数据\n"
                "2. 数据分析 — 指标对比、趋势分析、异常检测与归因分析\n"
                "3. 故障排查 — 数据 Pipeline 排障，包括 Kafka、Flink、日志查询等\n"
                "4. 执行决策 — 多步任务规划与自动化执行\n\n"
                "注意：当前模式下无法直接连接数据库执行 SQL。如果用户提供 SQL 请求审查或报错分析，请基于 SQL 语法规范进行静态分析，指出可能的语法错误、逻辑问题或优化建议。\n"
                "严禁编造任何查询结果或数据。\n\n"
                "可用数据表结构（编写 SQL 时必须严格使用以下表名和字段名，不得虚构）：\n"
                f"{SCHEMA_TEXT}\n\n"
                "请用专业清晰的方式回答，适当展开说明。如果用户的问题与上述能力无关，友好地引导回到数据相关话题。"
            )
            history = ctx.conversation_history or []
            messages = [{"role": "system", "content": _SYSTEM_CHAT_PROMPT}] + history + [{"role": "user", "content": message}]
            usage = {}
            answer = await chat_completion(messages, temperature=0.7, collect_usage=usage)
            # When confidence is low, append guidance directly (don't rely on LLM)
            if not intent_confidence:
                answer += "\n\n💡 **提问可能不够明确，参考以下示例重新提问：**\n"
                for label, q in IntentClassifier.GUIDANCE_EXAMPLES:
                    answer += f"- {label}：{q}\n"
            if usage:
                llm_usage["llm_calls"] = 1
                llm_usage["prompt_tokens"] = usage.get("prompt_tokens", 0)
                llm_usage["completion_tokens"] = usage.get("completion_tokens", 0)
                llm_usage["total_tokens"] = usage.get("total_tokens", 0)
                llm_usage["llm_latency_ms"] = usage.get("latency_ms", 0)
            final_answer = answer
            yield f"event: final_answer\ndata: {json.dumps({'answer': final_answer})}\n\n"

        elif PLANNER_MODE == "langgraph":
            yield f"event: thinking\ndata: {json.dumps({'message': '正在规划查询步骤...'})}\n\n"

            from src.agent.planner_run import planner_run_stream
            async for event in planner_run_stream(ctx, message):
                yield f"event: {event['event']}\ndata: {event['data']}\n\n"
                if event["event"] == "final_answer":
                    data = json.loads(event["data"])
                    final_answer = data.get("answer", "")

        else:
            from src.agent.react import react_run_stream
            async for event in react_run_stream(ctx, message):
                yield f"event: {event['event']}\ndata: {event['data']}\n\n"
                if event["event"] == "final_answer":
                    data = json.loads(event["data"])
                    final_answer = data.get("answer", "")

        # Log tool calls
        for step in ctx.step_results:
            log = ToolLog(
                session_id=req.session_id,
                trace_id=trace_id,
                tool_name=step["tool"],
                input_params=step["input"],
                output_data=step["output"],
                status="success" if step["success"] else "failed",
                latency_ms=step.get("latency_ms"),
                retry_count=ctx.retry_count,
            )
            db.add(log)
        await db.commit()

        # Save tool_results and analysis context for next round
        if ctx.step_results:
            await SessionMemory.save_tool_results(req.session_id, req.user_id, ctx.step_results)
            analysis_ctx = _extract_analysis_context(ctx.step_results)
            if analysis_ctx:
                await SessionMemory.save_analysis_context(req.session_id, req.user_id, analysis_ctx)

        # Save messages
        if final_answer:
            await SessionMemory.save_message(req.session_id, req.user_id, "user", message)
            await SessionMemory.save_message(req.session_id, req.user_id, "assistant", final_answer)

            round_count = await SessionMemory.get_round_count(req.session_id, req.user_id)
            if round_count >= SUMMARY_AFTER_ROUNDS:
                full_history = await SessionMemory.load_history(req.session_id, req.user_id)
                summary = await generate_summary(full_history)
                await SessionMemory.save_summary(req.session_id, req.user_id, summary)
                result = await db.execute(select(ChatSession).where(ChatSession.session_id == req.session_id))
                session = result.scalar_one()
                session.context_summary = summary
                await db.commit()

        # Aggregate tool performance metrics
        tool_latency_ms = []
        tool_success_count = 0
        tool_fail_count = 0
        for step in ctx.step_results:
            lat = step.get("latency_ms")
            if lat is not None:
                tool_latency_ms.append(lat)
            if step.get("success"):
                tool_success_count += 1
            else:
                tool_fail_count += 1
            if step.get("from_cache"):
                cache_hits += 1
            elif lat is not None:  # only count cache status for DB-backed tools
                cache_misses += 1

        # Aggregate LLM usage from ctx (planner/react paths) or direct path
        if ctx.llm_usage:
            for u in ctx.llm_usage:
                llm_usage["llm_calls"] += 1
                llm_usage["prompt_tokens"] += u.get("prompt_tokens", 0)
                llm_usage["completion_tokens"] += u.get("completion_tokens", 0)
                llm_usage["total_tokens"] += u.get("total_tokens", 0)
                llm_usage["llm_latency_ms"] += u.get("latency_ms", 0)

        total_elapsed_ms = round((datetime.utcnow() - start_time).total_seconds() * 1000, 1)

        # Estimate cost: rough RMB pricing per 1M tokens
        input_price_per_1m = 0.014  # ~qwen-plus input price
        output_price_per_1m = 0.07  # ~qwen-plus output price
        estimated_cost = round(
            llm_usage["prompt_tokens"] * input_price_per_1m / 1_000_000
            + llm_usage["completion_tokens"] * output_price_per_1m / 1_000_000,
            4,
        )

        # Aggregate reasoning metrics (Phase 3)
        reasoning_metrics = {}
        if ctx.step_results:
            # Count subtask results
            subtask_results = [s for s in ctx.step_results if s.get("metadata", {}).get("subtask_id")]
            if subtask_results:
                reasoning_metrics["subtasks_total"] = len(subtask_results)
                reasoning_metrics["subtasks_completed"] = sum(1 for s in subtask_results if s.get("success"))
            # Count failures (for backtrack tracking)
            failed_steps = [s for s in ctx.step_results if not s.get("success")]
            if failed_steps:
                reasoning_metrics["tool_failures"] = len(failed_steps)
        # Beam search info from context
        if hasattr(ctx, "llm_usage") and ctx.llm_usage:
            total_ll_calls = sum(u.get("llm_calls", 1) for u in ctx.llm_usage)
            if total_ll_calls > PLANNER_MAX_TOOL_CALLS:
                reasoning_metrics["extra_llm_calls_for_tot"] = total_ll_calls - PLANNER_MAX_TOOL_CALLS

        metrics_data = {
            "total_elapsed_ms": total_elapsed_ms,
            "llm_calls": llm_usage["llm_calls"],
            "prompt_tokens": llm_usage["prompt_tokens"],
            "completion_tokens": llm_usage["completion_tokens"],
            "total_tokens": llm_usage["total_tokens"],
            "llm_latency_ms": round(llm_usage["llm_latency_ms"], 1),
            "tool_calls": len(ctx.step_results),
            "tool_latency_ms": round(sum(tool_latency_ms) / len(tool_latency_ms), 1) if tool_latency_ms else 0,
            "tool_success": tool_success_count,
            "tool_fail": tool_fail_count,
            "cache_hits": cache_hits,
            "cache_misses": cache_misses,
            "estimated_cost": estimated_cost,
            **reasoning_metrics,
        }
        yield f"event: metrics\ndata: {json.dumps(metrics_data)}\n\n"

        _log.debug("answer  content=%s", final_answer if final_answer else "")
        record_chat("success", total_elapsed_ms / 1000)

        # Emit guidance suggestions when confidence is low
        if not intent_confidence:
            guidance_data = {
                "message": "💡 提问可能不够明确，您可以参考以下示例重新提问：",
                "examples": [{"label": label, "question": q} for label, q in IntentClassifier.GUIDANCE_EXAMPLES],
            }
            yield f"event: guidance\ndata: {json.dumps(guidance_data)}\n\n"

        yield f"event: done\ndata: {json.dumps({'ok': True})}\n\n"

    except Exception as e:
        _log.exception("SSE stream failed")
        yield f"event: error\ndata: {json.dumps({'message': str(e)})}\n\n"
        yield f"event: done\ndata: {json.dumps({'ok': False})}\n\n"


@app.post("/api/v1/chat/stream")
async def chat_stream(
    req: ChatRequest,
    token: str = Depends(_extract_token),
    db: AsyncSession = Depends(get_db),
):
    start = datetime.utcnow()

    _ = extract_user_id(token)  # JWT validation

    if is_rate_limited(req.user_id):
        raise HTTPException(status_code=429, detail="Rate limit exceeded")

    intent, intent_confidence = await IntentClassifier.classify_with_confidence(req.message)

    session = await db.get(ChatSession, req.session_id)
    if not session:
        session = ChatSession(session_id=req.session_id, user_id=req.user_id, intent_type=intent)
        db.add(session)
    else:
        session.intent_type = intent
        session.updated_at = datetime.utcnow()
    await db.commit()

    trace_id = str(uuid.uuid4())

    history = await SessionMemory.load_full_context(req.session_id, req.user_id)

    ctx = ExecutionContext(session_id=req.session_id, user_id=req.user_id)
    ctx.conversation_history = history
    ctx.prior_tool_results = await SessionMemory.load_tool_results(req.session_id, req.user_id)
    ctx.prior_analysis = await SessionMemory.load_analysis_context(req.session_id, req.user_id)

    _log.debug("Returning streaming response")
    return StreamingResponse(
        _sse_generator(ctx, req.message, db, req, trace_id, start, intent, intent_confidence),
        media_type="text/event-stream",
    )


@app.get("/api/v1/sessions")
async def list_sessions(
    user_id: str = Query(...),
    token: str = Depends(_extract_token),
    db: AsyncSession = Depends(get_db),
):
    if not validate_jwt(token):
        raise HTTPException(status_code=401, detail="Invalid token")

    result = await db.execute(select(ChatSession).where(ChatSession.user_id == user_id).order_by(ChatSession.updated_at.desc()))
    sessions = result.scalars().all()
    return [
        {
            "session_id": s.session_id,
            "intent_type": s.intent_type,
            "status": s.status,
            "created_at": str(s.created_at),
            "updated_at": str(s.updated_at),
        }
        for s in sessions
    ]


@app.get("/api/v1/sessions/{session_id}")
async def get_session(
    session_id: str,
    token: str = Depends(_extract_token),
    db: AsyncSession = Depends(get_db),
):
    if not validate_jwt(token):
        raise HTTPException(status_code=401, detail="Invalid token")

    session = await db.get(ChatSession, session_id)
    if not session:
        raise HTTPException(status_code=404, detail="Session not found")
    return {
        "session_id": session.session_id,
        "user_id": session.user_id,
        "intent_type": session.intent_type,
        "status": session.status,
        "created_at": str(session.created_at),
        "updated_at": str(session.updated_at),
        "context_summary": session.context_summary,
    }


@app.delete("/api/v1/sessions/{session_id}")
async def delete_session(
    session_id: str,
    token: str = Depends(_extract_token),
    db: AsyncSession = Depends(get_db),
):
    if not validate_jwt(token):
        raise HTTPException(status_code=401, detail="Invalid token")

    session = await db.get(ChatSession, session_id)
    if not session:
        raise HTTPException(status_code=404, detail="Session not found")

    await db.execute(delete(ToolLog).where(ToolLog.session_id == session_id))
    await db.delete(session)
    await db.commit()
    return {"message": "Session deleted"}


@app.get("/api/v1/logs")
async def get_logs(
    session_id: str = Query(...),
    token: str = Depends(_extract_token),
    db: AsyncSession = Depends(get_db),
):
    if not validate_jwt(token):
        raise HTTPException(status_code=401, detail="Invalid token")

    result = await db.execute(select(ToolLog).where(ToolLog.session_id == session_id).order_by(ToolLog.created_at))
    logs = result.scalars().all()
    return [
        {
            "id": log.id,
            "session_id": log.session_id,
            "trace_id": log.trace_id,
            "tool_name": log.tool_name,
            "input_params": log.input_params,
            "output_data": log.output_data,
            "status": log.status,
            "error_message": log.error_message,
            "latency_ms": log.latency_ms,
            "created_at": str(log.created_at),
        }
        for log in logs
    ]


@app.get("/metrics")
async def metrics(request: Request):
    """Expose Prometheus metrics."""
    return prometheus_metrics_endpoint(request)
