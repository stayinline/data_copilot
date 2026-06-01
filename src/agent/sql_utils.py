import re


def extract_user_sql(query: str) -> str | None:
    """Extract an embedded SQL statement from a user query."""
    sql_match = re.search(
        r"(SELECT|select|INSERT|insert|UPDATE|update|DELETE|delete|CREATE|create|DROP|drop)\b"
        r"\s+.*",
        query,
        re.DOTALL,
    )
    if not sql_match:
        return None

    extracted = sql_match.group(0).strip()
    extracted = re.split(r"\s*```", extracted)[0]
    extracted = re.split(r"\s*\n\s*(帮我|请|看看|分析|检查|谢谢)", extracted)[0]
    extracted = extracted.rstrip(" ,;")
    return extracted if len(extracted) > 10 else None
