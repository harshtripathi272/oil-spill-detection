import hashlib
import json
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

import numpy as np
import pandas as pd


@dataclass
class ModelScore:
    score: float
    label: str
    metadata: Dict[str, Any] = field(default_factory=dict)


class AISInferenceScoreModel:
    """Model wrapper that serves scores produced by preprocessing.ais_inference."""

    def __init__(
        self,
        model_name: str,
        scores_path: str,
        score_match_window_sec: int = 21600,
        reload_interval_sec: int = 300,
    ):
        self.model_name = model_name
        self.scores_path = Path(scores_path)
        self.score_match_window_sec = max(0, int(score_match_window_sec))
        self.reload_interval_sec = max(1, int(reload_interval_sec))
        self._last_loaded_at: Optional[datetime] = None
        self._last_mtime_ns: Optional[int] = None
        self._scores_df: Optional[pd.DataFrame] = None
        self._by_mmsi: Dict[str, pd.DataFrame] = {}

    def _ensure_scores_loaded(self) -> None:
        now = datetime.utcnow()
        should_reload = self._scores_df is None

        if self._last_loaded_at is not None and not should_reload:
            delta = (now - self._last_loaded_at).total_seconds()
            should_reload = delta >= self.reload_interval_sec

        if not should_reload:
            return

        if not self.scores_path.exists():
            self._scores_df = pd.DataFrame()
            self._by_mmsi = {}
            self._last_loaded_at = now
            self._last_mtime_ns = None
            return

        stat = self.scores_path.stat()
        mtime_ns = int(stat.st_mtime_ns)
        if self._scores_df is not None and self._last_mtime_ns == mtime_ns:
            self._last_loaded_at = now
            return

        df = pd.read_parquet(self.scores_path)
        required = {"mmsi", "start_timestamp", "combined_score"}
        if not required.issubset(df.columns):
            missing = sorted(required - set(df.columns))
            raise ValueError(f"AIS inference scores missing required columns: {missing}")

        work = df.copy()
        work["mmsi"] = work["mmsi"].astype(str)
        work["start_timestamp"] = pd.to_datetime(work["start_timestamp"], utc=True, errors="coerce")
        work = work.dropna(subset=["start_timestamp", "combined_score"]).reset_index(drop=True)

        grouped: Dict[str, pd.DataFrame] = {}
        for mmsi, grp in work.groupby("mmsi", sort=False):
            grouped[str(mmsi)] = grp.sort_values("start_timestamp").reset_index(drop=True)

        self._scores_df = work
        self._by_mmsi = grouped
        self._last_loaded_at = now
        self._last_mtime_ns = mtime_ns

    def infer(self, feature_event: Dict[str, Any]) -> ModelScore:
        self._ensure_scores_loaded()

        vessel_id = feature_event.get("vessel_id")
        ts_raw = feature_event.get("timestamp")
        if vessel_id is None or ts_raw is None:
            return ModelScore(score=0.0, label="normal", metadata={"reason": "missing_vessel_or_timestamp"})

        mmsi = str(vessel_id)
        ts = pd.to_datetime(str(ts_raw), utc=True, errors="coerce")
        if pd.isna(ts):
            return ModelScore(score=0.0, label="normal", metadata={"reason": "invalid_timestamp"})

        vessel_df = self._by_mmsi.get(mmsi)
        if vessel_df is None or vessel_df.empty:
            return ModelScore(score=0.0, label="normal", metadata={"reason": "mmsi_not_found"})

        ref_ts = vessel_df["start_timestamp"]
        delta_sec = (ref_ts - ts).dt.total_seconds().abs().to_numpy(dtype=np.float64)
        idx = int(np.argmin(delta_sec))
        best_delta = float(delta_sec[idx])
        if best_delta > self.score_match_window_sec:
            return ModelScore(
                score=0.0,
                label="normal",
                metadata={"reason": "no_match_within_window", "delta_sec": best_delta},
            )

        matched = vessel_df.iloc[idx]
        score = float(matched.get("combined_score", 0.0))
        score = max(0.0, min(score, 1.0))

        label = "anomalous" if bool(matched.get("is_anomaly", score >= 0.8)) else "normal"
        return ModelScore(
            score=score,
            label=label,
            metadata={
                "matched_start_timestamp": str(matched.get("start_timestamp")),
                "delta_sec": best_delta,
            },
        )


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
            "metadata": model_score.metadata,
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
