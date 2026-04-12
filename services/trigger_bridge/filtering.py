import hashlib
import json
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Tuple


def should_forward_event(anomaly_event: Dict[str, Any], threshold: float, allowed_bbox: str) -> Tuple[bool, Optional[str]]:
    score = _to_float(anomaly_event.get("score"))
    if score is None:
        return False, "Missing or invalid anomaly score"

    if score < threshold:
        return False, f"Score {score} below threshold {threshold}"

    lat = _to_float(anomaly_event.get("lat"))
    lon = _to_float(anomaly_event.get("lon"))
    if lat is None or lon is None:
        return False, "Missing or invalid lat/lon"

    if allowed_bbox.strip():
        bbox = _parse_bbox(allowed_bbox)
        if bbox is None:
            return False, "Invalid SAR_TRIGGER_ALLOWED_BBOX format"

        min_lon, min_lat, max_lon, max_lat = bbox
        if not (min_lat <= lat <= max_lat and min_lon <= lon <= max_lon):
            return False, "Anomaly outside allowed trigger bounding box"

    return True, None


def build_trigger_event(anomaly_event: Dict[str, Any]) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    vessel_id = anomaly_event.get("vessel_id")
    lat = _to_float(anomaly_event.get("lat"))
    lon = _to_float(anomaly_event.get("lon"))
    timestamp = _normalize_iso8601(str(anomaly_event.get("timestamp", "")))

    if vessel_id is None or lat is None or lon is None or not timestamp:
        return None, "Missing required fields to build trigger event"

    anomaly_event_id = anomaly_event.get("event_id")
    incident_id = f"inc-{anomaly_event_id[:12]}" if isinstance(anomaly_event_id, str) else _fallback_incident_id(vessel_id, timestamp)

    trigger_event = {
        "schema_version": "1.0",
        "event_type": "sar.trigger.event",
        "event_id": _deterministic_id(incident_id=incident_id, timestamp=timestamp, lat=lat, lon=lon),
        "incident_id": incident_id,
        "vessel_id": str(vessel_id),
        "lat": lat,
        "lon": lon,
        "timestamp": timestamp,
        "source_anomaly_event_id": anomaly_event_id,
        "score": anomaly_event.get("score"),
        "anomaly_type": anomaly_event.get("anomaly_type", "model_detected"),
        "model": anomaly_event.get("model", {}),
        "created_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "raw": anomaly_event,
    }
    return trigger_event, None


def _fallback_incident_id(vessel_id: Any, timestamp: str) -> str:
    raw = f"{vessel_id}|{timestamp}"
    return f"inc-{hashlib.sha256(raw.encode('utf-8')).hexdigest()[:12]}"


def _deterministic_id(incident_id: str, timestamp: str, lat: float, lon: float) -> str:
    payload = {
        "incident_id": incident_id,
        "timestamp": timestamp,
        "lat": lat,
        "lon": lon,
        "event_type": "sar.trigger.event",
    }
    raw = json.dumps(payload, sort_keys=True)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _parse_bbox(raw: str) -> Optional[Tuple[float, float, float, float]]:
    parts = [p.strip() for p in raw.split(",")]
    if len(parts) != 4:
        return None

    try:
        min_lon, min_lat, max_lon, max_lat = [float(p) for p in parts]
    except ValueError:
        return None

    if min_lon > max_lon or min_lat > max_lat:
        return None

    return min_lon, min_lat, max_lon, max_lat


def _normalize_iso8601(value: str) -> Optional[str]:
    if not value.strip():
        return None

    normalized = value.replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(normalized)
    except ValueError:
        return None

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)

    return dt.isoformat().replace("+00:00", "Z")


def _to_float(value: Any) -> Optional[float]:
    try:
        if value is None:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None
