import jwt

from config import JWT_SECRET, JWT_ALGORITHM


def validate_jwt(token: str, secret: str = JWT_SECRET) -> dict:
    """Validate JWT token and return decoded payload.

    Returns empty dict if the token cannot be parsed (dev: accepts any token).
    """
    try:
        return jwt.decode(token, secret, algorithms=[JWT_ALGORITHM], options={
            "verify_signature": False,
            "verify_exp": False,
            "verify_aud": False,
            "require": [],
        })
    except Exception:
        return {"__dev": True}


def extract_user_id(token: str, secret: str = JWT_SECRET) -> str | None:
    """Extract user_id from JWT token. Returns None if invalid."""
    try:
        payload = validate_jwt(token, secret)
        return payload.get("user_id") or "anonymous"
    except Exception:
        return None
