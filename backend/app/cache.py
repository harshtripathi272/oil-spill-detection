"""
Lightweight Redis cache wrapper with graceful in-memory fallback.
If Redis is unavailable the cache is a simple TTL dict — nothing breaks.
"""

import json
import time
import logging
from typing import Any, Optional

logger = logging.getLogger(__name__)

# ── Try to connect to Redis ───────────────────────────────────────────────────
_redis_client = None

def _get_redis():
    global _redis_client
    if _redis_client is not None:
        return _redis_client
    try:
        import redis
        r = redis.Redis(host="localhost", port=6379, db=0, socket_timeout=1)
        r.ping()
        _redis_client = r
        logger.info("✅ Redis cache connected at localhost:6379")
        return _redis_client
    except Exception as e:
        logger.warning(f"⚠️  Redis unavailable ({e}) — using in-memory fallback cache")
        return None


# ── In-memory fallback cache ──────────────────────────────────────────────────
_mem_cache: dict[str, tuple[Any, float]] = {}  # key → (value, expires_at)


def cache_get(key: str) -> Optional[Any]:
    """Fetch from Redis or memory fallback. Returns None on miss."""
    r = _get_redis()
    if r:
        try:
            raw = r.get(key)
            if raw is None:
                return None
            return json.loads(raw)
        except Exception:
            pass  # fall through to memory
    # memory fallback
    entry = _mem_cache.get(key)
    if entry and time.time() < entry[1]:
        return entry[0]
    _mem_cache.pop(key, None)
    return None


def cache_set(key: str, value: Any, ttl: int = 60) -> None:
    """Store in Redis (with TTL) or memory fallback."""
    r = _get_redis()
    if r:
        try:
            r.setex(key, ttl, json.dumps(value, default=str))
            return
        except Exception:
            pass
    _mem_cache[key] = (value, time.time() + ttl)


def cache_delete(key: str) -> None:
    """Delete a key from cache."""
    r = _get_redis()
    if r:
        try:
            r.delete(key)
        except Exception:
            pass
    _mem_cache.pop(key, None)


def cache_clear_prefix(prefix: str) -> None:
    """Delete all keys matching a prefix (Redis only; memory cache full clear not needed in practice)."""
    r = _get_redis()
    if r:
        try:
            for key in r.scan_iter(f"{prefix}*"):
                r.delete(key)
        except Exception:
            pass
    to_del = [k for k in _mem_cache if k.startswith(prefix)]
    for k in to_del:
        del _mem_cache[k]
