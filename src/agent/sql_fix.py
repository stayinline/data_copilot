import re

from src.agent.llm_client import chat_completion
from src.gateway.metrics import record_sql_auto_fix
from src.sql.schema_loader import SCHEMAS
from src.utils.logging import get_logger
from config import SQL_AUTO_FIX_MAX_RETRIES, CHAT_MODEL

_log = get_logger("sql_fix")

_FIX_SQL_PROMPT = """\
你是一个 SQL 专家。以下 SQL 执行失败了，请修复它。

**原始 SQL：**
{original_sql}

**错误信息：**
{error_message}

**可用表结构：**
{schema}

请只返回修复后的 SQL 语句，不要包含任何解释或 markdown 代码块标记。
确保修复后的 SQL 是有效的 SELECT 查询，仅从可用表中读取数据。
"""


def _strip_markdown(text: str) -> str:
    """Strip markdown code block markers from LLM output."""
    text = text.strip()
    match = re.search(r"```(?:sql)?\s*(.*?)\s*```", text, re.DOTALL)
    if match:
        return match.group(1).strip()
    return text


def _get_schema_text() -> str:
    lines = []
    for tname, cols in sorted(SCHEMAS.items()):
        col_list = ", ".join(f"{c}" for c, _ in cols)
        lines.append(f"- {tname}({col_list})")
    return "\n".join(lines)


async def fix_sql(original_sql: str, error_message: str, tables: str = "") -> str:
    """Call LLM to generate fixed SQL."""
    _log.debug("fix_sql  error=%s", error_message[:100])
    schema = _get_schema_text()
    prompt = _FIX_SQL_PROMPT.format(
        original_sql=original_sql,
        error_message=error_message,
        schema=schema,
    )
    messages = [{"role": "user", "content": prompt}]
    result = await chat_completion(messages, model=CHAT_MODEL, temperature=0.0)
    return _strip_markdown(result)


async def auto_fix_sql(
    original_sql: str,
    error_message: str,
    max_retries: int = SQL_AUTO_FIX_MAX_RETRIES,
    tables: str = "",
) -> tuple[str | None, str]:
    """Attempt to auto-fix SQL with LLM, up to max_retries attempts.

    Returns (fixed_sql, status) where status is "success" or "failed".
    """
    _log.debug("auto_fix_sql start  retries=%d", max_retries)
    current_error = error_message

    for attempt in range(max_retries):
        try:
            fixed_sql = await fix_sql(original_sql, current_error, tables)

            # Validate the fixed SQL is not empty
            if not fixed_sql or len(fixed_sql) < 5:
                _log.debug("auto_fix attempt %d: empty response", attempt + 1)
                current_error = "LLM returned empty response"
                continue

            _log.debug("auto_fix success  attempt=%d  sql_len=%d", attempt + 1, len(fixed_sql))
            record_sql_auto_fix("success")
            return fixed_sql, "success"
        except Exception as e:
            _log.debug("auto_fix attempt %d failed: %s", attempt + 1, str(e))
            current_error = str(e)

    _log.debug("auto_fix_sql all retries failed")
    record_sql_auto_fix("failed")
    return None, "failed"
