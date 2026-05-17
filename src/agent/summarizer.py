from src.agent.llm_client import chat_completion
from src.utils.logging import get_logger
from config import SUMMARY_MODEL, MAX_CONTEXT_ROUNDS

_log = get_logger("summarizer")

_SUMMARY_PROMPT = """\
请对以下用户与数据分析助手的对话进行简洁摘要。
摘要应包含：用户的核心问题、使用的SQL查询、关键数据结论。
用中文，控制在200字以内。

对话内容：
{conversation}

摘要："""


async def generate_summary(messages: list[dict]) -> str:
    """Compress conversation history into a summary using LLM."""
    _log.debug("generate_summary  msg_count=%d", len(messages))
    # Format messages for the prompt
    conversation = ""
    for msg in messages:
        role = "用户" if msg.get("role") == "user" else "助手"
        content = msg.get("content", "")
        # Truncate long content
        if len(content) > 500:
            content = content[:500] + "..."
        conversation += f"{role}: {content}\n"

    prompt = _SUMMARY_PROMPT.format(conversation=conversation)
    messages = [{"role": "user", "content": prompt}]
    result = await chat_completion(messages, model=SUMMARY_MODEL, temperature=0.0)
    return result.strip()
