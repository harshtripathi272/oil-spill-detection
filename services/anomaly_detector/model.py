import hashlib
import json
from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple


@dataclass
class ModelScore:
    score: float
    label: str


class PlaceholderTrajectoryModel:
    """
    Placeholder model wrapper for model-based anomaly scoring.

    This intentionally uses a lightweight deterministic scoring function so the
    service is runnable now while preserving a clear interface for replacing with
    a trained model artifact later.
    """

    def __init__(self, model_name: str):
        self.model_name = model_name

    def infer(self, feature_event: Dict[str, Any]) -> ModelScore:
        features = feature_event.get("features", {})

        speed = _to_float(features.get("speed_knots"), default=0.0)
        accel = abs(_to_float(features.get("acceleration_knots_per_sec"), default=0.0))
        heading_rate = abs(_to_float(features.get("heading_change_rate_deg_per_sec"), default=0.0))
        time_gap = _to_float(features.get("time_gap_sec"), default=0.0)

        # Deterministic score in [0, 1] combining abrupt motion and AIS silence.
        speed_component = min(speed / 40.0, 1.0)
        accel_component = min(accel / 0.2, 1.0)
        heading_component = min(heading_rate / 3.0, 1.0)
        gap_component = min(time_gap / 3600.0, 1.0)

        score = 0.15 * speed_component + 0.35 * accel_component + 0.25 * heading_component + 0.25 * gap_component
        score = max(0.0, min(score, 1.0))

        label = "anomalous" if score >= 0.5 else "normal"
        return ModelScore(score=score, label=label)


def build_anomaly_event(
    model_name: str,
    feature_event: Dict[str, Any],
    model_score: ModelScore,
) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    vessel_id = feature_event.get("vessel_id")
    lat = feature_event.get("lat")
    lon = feature_event.get("lon")
    timestamp = feature_event.get("timestamp")

    if vessel_id is None or lat is None or lon is None or timestamp is None:
        return None, "Missing required fields in feature event"

    try:
        lat = float(lat)
        lon = float(lon)
    except (TypeError, ValueError):
        return None, "Invalid lat/lon in feature event"

    event = {
        "schema_version": "1.0",
        "event_type": "ais.anomalies.event",
        "event_id": _deterministic_id(vessel_id=str(vessel_id), timestamp=str(timestamp), lat=lat, lon=lon),
        "vessel_id": str(vessel_id),
        "lat": lat,
        "lon": lon,
        "timestamp": str(timestamp),
        "anomaly_type": "model_detected",
        "score": model_score.score,
        "model": {
            "name": model_name,
            "label": model_score.label,
        },
        "source_event_id": feature_event.get("event_id"),
        "features": feature_event.get("features", {}),
        "raw": feature_event,
    }
    return event, None


def _deterministic_id(vessel_id: str, timestamp: str, lat: float, lon: float) -> str:
    payload = {
        "vessel_id": vessel_id,
        "timestamp": timestamp,
        "lat": lat,
        "lon": lon,
        "event_type": "ais.anomalies.event",
    }
    raw = json.dumps(payload, sort_keys=True)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _to_float(value: Any, default: float) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except (TypeError, ValueError):
        return default
