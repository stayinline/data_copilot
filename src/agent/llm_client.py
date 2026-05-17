import asyncio
import json
import time

import httpx

from config import (
    MODEL_API_KEY,
    MODEL_BASE_URL,
    CHAT_MODEL,
    SUMMARY_MODEL,
    EMBEDDING_MODEL,
    LLM_TIMEOUT,
)
from src.gateway.metrics import record_llm_call
from src.utils.logging import get_logger

_log = get_logger("llm")


async def chat_completion(
    messages: list[dict],
    model: str | None = None,
    temperature: float = 0.1,
    max_retries: int = 2,
    collect_usage: dict | None = None,
) -> str:
    """Call LLM chat completion API with retry.

    If collect_usage is provided (a dict), it will be populated with:
      - prompt_tokens, completion_tokens, total_tokens, model, latency_ms
    """
    model = model or CHAT_MODEL
    _log.debug("chat_completion  model=%s  msg_count=%d", model, len(messages))
    start = time.monotonic()
    timeout = httpx.Timeout(LLM_TIMEOUT, connect=30.0, pool=10.0)
    last_exc = None

    for attempt in range(max_retries + 1):
        if attempt > 0:
            _log.debug("chat_completion retry %d/%d", attempt, max_retries)
        try:
            async with httpx.AsyncClient(timeout=timeout) as client:
                resp = await client.post(
                    f"{MODEL_BASE_URL}/chat/completions",
                    headers={"Authorization": f"Bearer {MODEL_API_KEY}"},
                    json={
                        "model": model,
                        "messages": messages,
                        "temperature": temperature,
                    },
                )
                resp.raise_for_status()
                body = resp.json()
                content = body["choices"][0]["message"]["content"]
                if collect_usage is not None:
                    usage = body.get("usage", {})
                    elapsed_ms = (time.monotonic() - start) * 1000
                    collect_usage.update({
                        "prompt_tokens": usage.get("prompt_tokens", 0),
                        "completion_tokens": usage.get("completion_tokens", 0),
                        "total_tokens": usage.get("total_tokens", 0),
                        "model": body.get("model", model),
                        "latency_ms": round(elapsed_ms, 1),
                    })
                _log.debug("chat_completion success  content_len=%d", len(content))
                record_llm_call(time.monotonic() - start)
                return content
        except Exception as e:
            last_exc = e
            if attempt < max_retries:
                await asyncio.sleep(2 ** attempt)
    record_llm_call(time.monotonic() - start)
    raise last_exc


async def chat_completion_stream(
    messages: list[dict],
    model: str | None = None,
    temperature: float = 0.1,
):
    """Stream LLM chat completion via SSE."""
    model = model or CHAT_MODEL
    start = time.monotonic()
    try:
        async with httpx.AsyncClient(timeout=LLM_TIMEOUT) as client:
            async with client.stream(
                "POST",
                f"{MODEL_BASE_URL}/chat/completions",
                headers={"Authorization": f"Bearer {MODEL_API_KEY}"},
                json={
                    "model": model,
                    "messages": messages,
                    "temperature": temperature,
                    "stream": True,
                },
            ) as resp:
                resp.raise_for_status()
                async for line in resp.aiter_lines():
                    if not line or not line.startswith("data: "):
                        continue
                    data = line[6:]
                    if data == "[DONE]":
                        break
                    try:
                        chunk = json.loads(data)
                        delta = chunk["choices"][0]["delta"]
                        content = delta.get("content", "")
                        if content:
                            yield content
                    except (json.JSONDecodeError, KeyError, IndexError):
                        continue
    finally:
        record_llm_call(time.monotonic() - start)


class StreamingChatModel:
    """Minimal LangChain-compatible ChatModel wrapper for streaming."""

    def __init__(self, model: str | None = None, temperature: float = 0.1):
        self.model = model or CHAT_MODEL
        self.temperature = temperature

    async def astream(self, messages: list[dict], **kwargs):
        from langchain_core.messages import AIMessageChunk
        from langchain_core.outputs import ChatGenerationChunk

        async for token in chat_completion_stream(
            messages=[{"role": m["role"], "content": m["content"]} for m in messages],
            model=self.model,
            temperature=self.temperature,
        ):
            yield ChatGenerationChunk(
                message=AIMessageChunk(content=token),
                chunk=token,
            )


async def embed(texts: list[str], model: str | None = None) -> list[list[float]]:
    """Call embedding API with automatic batching (max 10 per request)."""
    model = model or EMBEDDING_MODEL
    start = time.monotonic()
    try:
        batch_size = 10
        all_embeddings: list[list[float]] = []
        async with httpx.AsyncClient(timeout=LLM_TIMEOUT) as client:
            for i in range(0, len(texts), batch_size):
                batch = texts[i:i + batch_size]
                resp = await client.post(
                    f"{MODEL_BASE_URL}/embeddings",
                    headers={"Authorization": f"Bearer {MODEL_API_KEY}"},
                    json={"model": model, "input": batch},
                )
                resp.raise_for_status()
                data = resp.json()
                all_embeddings.extend(item["embedding"] for item in data["data"])
        return all_embeddings
    finally:
        record_llm_call(time.monotonic() - start)


def select_model_for_task(intent: str, task_type: str = "") -> str:
    """Route to the appropriate model based on task complexity.

    - planning / analysis -> qwen3.6-plus (CHAT_MODEL)
    - summary / intent / CHAT -> qwen-turbo (SUMMARY_MODEL)
    - embedding -> text-embedding-v4
    """
    if task_type == "embedding":
        return EMBEDDING_MODEL

    simple_tasks = {"summary", "intent", "chat"}
    if intent.upper() == "CHAT" or task_type.lower() in simple_tasks:
        return SUMMARY_MODEL

    # Default to the main chat model for complex tasks
    return CHAT_MODEL
