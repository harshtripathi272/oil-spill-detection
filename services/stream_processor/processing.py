import hashlib
import json
import logging
import math
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Tuple

logger = logging.getLogger(__name__)


def validate_and_normalize(raw_msg: Dict[str, Any]) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    if not isinstance(raw_msg, dict):
        return None, "Payload must be a JSON object"

    msg_type = _extract_message_type(raw_msg)
    if msg_type != "PositionReport":
        return None, "MessageType must be PositionReport"

    vessel_id = _extract_vessel_id(raw_msg)
    if not vessel_id:
        return None, "Missing vessel identifier (MMSI)"

    lat, lon = _extract_lat_lon(raw_msg)
    if lat is None or lon is None:
        return None, "Missing latitude/longitude"

    if not (-90 <= lat <= 90) or not (-180 <= lon <= 180):
        return None, "Coordinates out of range"

    timestamp = _extract_timestamp(raw_msg)
    if not timestamp:
        return None, "Missing timestamp"

    normalized_ts = _normalize_iso8601(timestamp)
    if not normalized_ts:
        return None, "Invalid timestamp format"

    heading = _extract_heading(raw_msg)
    reported_speed = _extract_speed_knots(raw_msg)

    cleaned = {
        "schema_version": "1.0",
        "event_type": "ais.cleaned.position_report",
        "event_id": _deterministic_id(vessel_id, normalized_ts, lat, lon),
        "vessel_id": vessel_id,
        "timestamp": normalized_ts,
        "lat": lat,
        "lon": lon,
        "heading_deg": heading,
        "speed_knots_reported": reported_speed,
        "source": "ais.raw.position_reports",
        "raw": raw_msg,
    }

    return cleaned, None


def build_feature_event(cleaned: Dict[str, Any], state: Dict[str, Any]) -> Dict[str, Any]:
    vessel_id = cleaned["vessel_id"]
    current_ts = cleaned["timestamp"]
    current_lat = cleaned["lat"]
    current_lon = cleaned["lon"]
    current_heading = cleaned.get("heading_deg")

    prev_lat, prev_lon, prev_ts, prev_speed, prev_heading = _last_observation(state)

    computed_speed = None
    if prev_lat is not None and prev_lon is not None and prev_ts:
        gap_sec = _seconds_between(prev_ts, current_ts)
        if gap_sec and gap_sec > 0:
            distance_km = _haversine_km(prev_lat, prev_lon, current_lat, current_lon)
            computed_speed = (distance_km / gap_sec) * 3600 / 1.852

    speed_knots = cleaned.get("speed_knots_reported")
    if speed_knots is None:
        speed_knots = computed_speed

    time_gap_sec = _seconds_between(prev_ts, current_ts) if prev_ts else None

    acceleration_knots_per_sec = None
    if speed_knots is not None and prev_speed is not None and time_gap_sec and time_gap_sec > 0:
        acceleration_knots_per_sec = (speed_knots - prev_speed) / time_gap_sec

    heading_change_rate_deg_per_sec = None
    if current_heading is not None and prev_heading is not None and time_gap_sec and time_gap_sec > 0:
        delta = _smallest_angle_delta(prev_heading, current_heading)
        heading_change_rate_deg_per_sec = delta / time_gap_sec

    state["last_positions"].append({"lat": current_lat, "lon": current_lon})
    state["timestamps"].append(current_ts)
    state["speeds_knots"].append(speed_knots)
    state["headings_deg"].append(current_heading)

    return {
        "schema_version": "1.0",
        "event_type": "ais.features.vessel_track",
        "event_id": _deterministic_id(vessel_id, current_ts, current_lat, current_lon, suffix="features"),
        "vessel_id": vessel_id,
        "timestamp": current_ts,
        "lat": current_lat,
        "lon": current_lon,
        "features": {
            "speed_knots": speed_knots,
            "acceleration_knots_per_sec": acceleration_knots_per_sec,
            "heading_change_rate_deg_per_sec": heading_change_rate_deg_per_sec,
            "time_gap_sec": time_gap_sec,
            "trajectory_window": list(state["last_positions"]),
            "timestamp_window": list(state["timestamps"]),
        },
        "source_event_id": cleaned["event_id"],
    }


def _extract_message_type(msg: Dict[str, Any]) -> Optional[str]:
    msg_type = msg.get("MessageType")
    if isinstance(msg_type, str):
        return msg_type
    if isinstance(msg_type, dict):
        if "PositionReport" in msg_type:
            return "PositionReport"
    return None


def _extract_vessel_id(msg: Dict[str, Any]) -> Optional[str]:
    metadata = msg.get("MetaData", {}) if isinstance(msg.get("MetaData"), dict) else {}
    mmsi = metadata.get("MMSI") or metadata.get("mmsi") or msg.get("mmsi")
    return str(mmsi) if mmsi is not None else None


def _extract_lat_lon(msg: Dict[str, Any]) -> Tuple[Optional[float], Optional[float]]:
    pr = _position_report_payload(msg)
    lat = pr.get("Latitude") if isinstance(pr, dict) else None
    lon = pr.get("Longitude") if isinstance(pr, dict) else None

    if lat is None:
        lat = msg.get("lat") or msg.get("latitude")
    if lon is None:
        lon = msg.get("lon") or msg.get("longitude")

    try:
        return float(lat), float(lon)
    except (TypeError, ValueError):
        return None, None


def _extract_timestamp(msg: Dict[str, Any]) -> Optional[str]:
    pr = _position_report_payload(msg)
    ts = None
    if isinstance(pr, dict):
        ts = pr.get("Timestamp")
    ts = ts or msg.get("timestamp") or msg.get("time") or msg.get("ts")
    return str(ts) if ts is not None else None


def _extract_heading(msg: Dict[str, Any]) -> Optional[float]:
    pr = _position_report_payload(msg)
    value = None
    if isinstance(pr, dict):
        value = pr.get("TrueHeading")
        if value is None:
            value = pr.get("Cog")
        if value is None:
            value = pr.get("COG")

    try:
        return float(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def _extract_speed_knots(msg: Dict[str, Any]) -> Optional[float]:
    pr = _position_report_payload(msg)
    value = None
    if isinstance(pr, dict):
        value = pr.get("Sog")
        if value is None:
            value = pr.get("SOG")

    try:
        return float(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def _position_report_payload(msg: Dict[str, Any]) -> Dict[str, Any]:
    message = msg.get("Message") if isinstance(msg.get("Message"), dict) else {}
    pr = message.get("PositionReport")
    return pr if isinstance(pr, dict) else {}


def _normalize_iso8601(value: str) -> Optional[str]:
    raw = value.strip()
    if not raw:
        return None

    normalized = raw.replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(normalized)
    except ValueError:
        return None

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)

    return dt.isoformat().replace("+00:00", "Z")


def _seconds_between(prev_iso: str, curr_iso: str) -> Optional[float]:
    if not prev_iso or not curr_iso:
        return None
    try:
        prev = datetime.fromisoformat(prev_iso.replace("Z", "+00:00"))
        curr = datetime.fromisoformat(curr_iso.replace("Z", "+00:00"))
    except ValueError:
        return None
    return max((curr - prev).total_seconds(), 0.0)


def _haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    r = 6371.0
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    d_phi = math.radians(lat2 - lat1)
    d_lambda = math.radians(lon2 - lon1)

    a = math.sin(d_phi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(d_lambda / 2) ** 2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return r * c


def _smallest_angle_delta(a: float, b: float) -> float:
    delta = (b - a + 180) % 360 - 180
    return delta


def _last_observation(state: Dict[str, Any]) -> Tuple[Optional[float], Optional[float], Optional[str], Optional[float], Optional[float]]:
    last_positions = state.get("last_positions")
    timestamps = state.get("timestamps")
    speeds = state.get("speeds_knots")
    headings = state.get("headings_deg")

    if not last_positions or not timestamps:
        return None, None, None, None, None

    prev_pos = last_positions[-1]
    prev_ts = timestamps[-1]
    prev_speed = speeds[-1] if speeds else None
    prev_heading = headings[-1] if headings else None

    return prev_pos.get("lat"), prev_pos.get("lon"), prev_ts, prev_speed, prev_heading


def _deterministic_id(vessel_id: str, timestamp: str, lat: float, lon: float, suffix: str = "cleaned") -> str:
    raw = json.dumps(
        {
            "vessel_id": vessel_id,
            "timestamp": timestamp,
            "lat": lat,
            "lon": lon,
            "suffix": suffix,
        },
        sort_keys=True,
    )
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()
