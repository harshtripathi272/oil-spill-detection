import json
import logging
from collections import deque
from typing import Any, Deque, Dict, List

logger = logging.getLogger(__name__)


class InMemoryVesselStateStore:
    def __init__(self, window_size: int):
        self.window_size = window_size
        self._state: Dict[str, Dict[str, Any]] = {}

    def get(self, vessel_id: str) -> Dict[str, Any]:
        return self._state.get(vessel_id, _empty_state(self.window_size))

    def set(self, vessel_id: str, state: Dict[str, Any]) -> None:
        self._state[vessel_id] = state


class RedisVesselStateStore:
    def __init__(self, redis_url: str, window_size: int, ttl_sec: int):
        self.window_size = window_size
        self.ttl_sec = ttl_sec

        try:
            from redis import Redis
        except Exception as exc:
            raise RuntimeError("redis package is not installed") from exc

        self._redis = Redis.from_url(redis_url, decode_responses=True)
        # Fail fast so the service can fall back to in-memory state if Redis is unavailable.
        self._redis.ping()

    def _key(self, vessel_id: str) -> str:
        return f"vessel:{vessel_id}"

    def get(self, vessel_id: str) -> Dict[str, Any]:
        raw = self._redis.get(self._key(vessel_id))
        if not raw:
            return _empty_state(self.window_size)
        try:
            payload = json.loads(raw)
            return _normalize_state(payload, self.window_size)
        except Exception:
            logger.warning("Failed to decode vessel state for %s; resetting state", vessel_id)
            return _empty_state(self.window_size)

    def set(self, vessel_id: str, state: Dict[str, Any]) -> None:
        payload = json.dumps(_serialize_state(state))
        key = self._key(vessel_id)
        self._redis.set(key, payload)
        self._redis.expire(key, self.ttl_sec)


class VesselStateManager:
    def __init__(self, backend: str, redis_url: str, window_size: int, ttl_sec: int):
        self.window_size = window_size

        if backend.lower() == "redis":
            try:
                self.store = RedisVesselStateStore(
                    redis_url=redis_url,
                    window_size=window_size,
                    ttl_sec=ttl_sec,
                )
                logger.info("Using Redis-backed vessel state store")
                return
            except Exception as exc:
                logger.warning("Redis unavailable (%s); falling back to in-memory state", exc)

        self.store = InMemoryVesselStateStore(window_size=window_size)
        logger.info("Using in-memory vessel state store")

    def get_state(self, vessel_id: str) -> Dict[str, Any]:
        return self.store.get(vessel_id)

    def put_state(self, vessel_id: str, state: Dict[str, Any]) -> None:
        self.store.set(vessel_id, _normalize_state(state, self.window_size))


def _empty_state(window_size: int) -> Dict[str, Any]:
    return {
        "last_positions": deque(maxlen=window_size),
        "timestamps": deque(maxlen=window_size),
        "speeds_knots": deque(maxlen=window_size),
        "headings_deg": deque(maxlen=window_size),
        "cogs_deg": deque(maxlen=window_size),
    }


def _normalize_state(state: Dict[str, Any], window_size: int) -> Dict[str, Any]:
    normalized = _empty_state(window_size)
    for key in normalized.keys():
        values = state.get(key, [])
        normalized[key].extend(list(values)[-window_size:])
    return normalized


def _serialize_state(state: Dict[str, Any]) -> Dict[str, List[Any]]:
    return {
        "last_positions": list(state.get("last_positions", [])),
        "timestamps": list(state.get("timestamps", [])),
        "speeds_knots": list(state.get("speeds_knots", [])),
        "headings_deg": list(state.get("headings_deg", [])),
        "cogs_deg": list(state.get("cogs_deg", [])),
    }
