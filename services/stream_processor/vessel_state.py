import json
import logging
from collections import deque
from typing import Any, Deque, Dict, List, Optional, Tuple, Union

logger = logging.getLogger(__name__)


# In-Memory Store 
class InMemoryVesselStateStore:
    def __init__(self, window_size: int, ttl_sec: int, max_vessels: int = 100000):
        from cachetools import TTLCache

        self.window_size = window_size
        self._state = TTLCache(maxsize=max_vessels, ttl=ttl_sec)

    def get(self, vessel_id: str) -> Dict[str, Any]:
        return self._state.get(vessel_id, _empty_state(self.window_size))

    def set(self, vessel_id: str, state: Dict[str, Any]) -> None:
        self._state[vessel_id] = state


# Redis Store 
class RedisVesselStateStore:
    def __init__(self, redis_url: str, window_size: int, ttl_sec: int):
        self.window_size = window_size
        self.ttl_sec = ttl_sec

        try:
            from redis import Redis
        except Exception as exc:
            raise RuntimeError("redis package is not installed") from exc

        self._redis = Redis.from_url(redis_url, decode_responses=True)

        # Fail fast if Redis unavailable
        self._redis.ping()

    def _key(self, vessel_id: str) -> str:
        return f"vessel:{vessel_id}"

    def get(self, vessel_id: str) -> Dict[str, Any]:
        raw = self._redis.get(self._key(vessel_id))
        if not raw:
            return _empty_state(self.window_size)

        try:
            payload = json.loads(raw)
            return _deserialize_state(payload, self.window_size)
        except Exception:
            logger.warning("Corrupted state for %s, resetting", vessel_id)
            return _empty_state(self.window_size)

    def set(self, vessel_id: str, state: Dict[str, Any]) -> None:
        payload = json.dumps(_serialize_state(state))

        # 🔥 Atomic set + TTL (fixes memory leak bug)
        self._redis.set(self._key(vessel_id), payload, ex=self.ttl_sec)



# Manager
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
                logger.warning(
                    "Redis unavailable (%s); falling back to in-memory", exc
                )

        # fallback (safe now)
        self.store = InMemoryVesselStateStore(
            window_size=window_size,
            ttl_sec=ttl_sec,
        )
        logger.info("Using in-memory vessel state store")

    def get_state(self, vessel_id: str) -> Dict[str, Any]:
        return self.store.get(vessel_id)

    def put_state(self, vessel_id: str, state: Dict[str, Any]) -> None:
        self.store.set(vessel_id, state)

# State Representation 
# Instead of 5 separate deques → use ONE compact structure
# Each entry = (lat, lon, timestamp, speed, heading, cog)
StateEntry = Tuple[float, float, Union[float, str], Optional[float], Optional[float], Optional[float]]


def _empty_state(window_size: int) -> Dict[str, Any]:
    return {
        "history": deque(maxlen=window_size)  # type: Deque[StateEntry]
    }


def append_state(
    state: Dict[str, Any],
    lat: float,
    lon: float,
    timestamp: Union[float, str],
    speed: Optional[float],
    heading: Optional[float],
    cog: Optional[float],
):
    state["history"].append((lat, lon, timestamp, speed, heading, cog))

# Serialization
def _serialize_state(state: Dict[str, Any]) -> Dict[str, List[Any]]:
    return {
        "history": list(state.get("history", []))
    }


def _deserialize_state(payload: Dict[str, Any], window_size: int) -> Dict[str, Any]:
    state = _empty_state(window_size)

    history = payload.get("history", [])
    state["history"].extend(history[-window_size:])

    return state