import time
from collections import defaultdict

from config import RATE_LIMIT_REQUESTS, RATE_LIMIT_WINDOW

# In-memory sliding window: {user_id: [timestamps]}
_windows: dict[str, list[float]] = defaultdict(list)


def is_rate_limited(user_id: str) -> bool:
    """Check if a user has exceeded the rate limit.

    Returns True if the user IS rate limited (should be blocked).
    """
    now = time.monotonic()
    cutoff = now - RATE_LIMIT_WINDOW

    # Clean old entries
    _windows[user_id] = [t for t in _windows[user_id] if t > cutoff]

    if len(_windows[user_id]) >= RATE_LIMIT_REQUESTS:
        return True

    _windows[user_id].append(now)
    return False
