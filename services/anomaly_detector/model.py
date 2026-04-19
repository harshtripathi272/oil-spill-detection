import hashlib
import json
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

import numpy as np
import pandas as pd
import torch

try:
    import faiss

    FAISS_AVAILABLE = True
except ImportError:
    faiss = None
    FAISS_AVAILABLE = False

from preprocessing.ais_contrastive_train import SequenceTransformerEncoder


@dataclass
class ModelScore:
    score: float
    label: str
    metadata: Dict[str, Any] = field(default_factory=dict)


def _l2_normalize(arr: np.ndarray, eps: float = 1e-12) -> np.ndarray:
    arr = np.asarray(arr, dtype=np.float32)
    norms = np.linalg.norm(arr, axis=1, keepdims=True)
    norms = np.maximum(norms, eps)
    return arr / norms


def _numpy_mean_knn_distance(bank: np.ndarray, queries: np.ndarray, k: int) -> np.ndarray:
    if queries.size == 0:
        return np.empty((0,), dtype=np.float32)
    if bank.size == 0:
        return np.ones((queries.shape[0],), dtype=np.float32)

    sims = queries @ bank.T
    k_eff = max(1, min(int(k), int(bank.shape[0])))
    topk = np.partition(sims, kth=sims.shape[1] - k_eff, axis=1)[:, -k_eff:]
    d = np.sqrt(np.maximum(2.0 - 2.0 * topk, 0.0)).astype(np.float32, copy=False)
    return d.mean(axis=1).astype(np.float32, copy=False)


def _faiss_mean_knn_distance(index: object, queries: np.ndarray, k: int) -> np.ndarray:
    if queries.size == 0:
        return np.empty((0,), dtype=np.float32)
    ntotal = int(index.ntotal)
    if ntotal == 0:
        return np.ones((queries.shape[0],), dtype=np.float32)

    k_eff = max(1, min(int(k), ntotal))
    d2, idx = index.search(queries.astype(np.float32, copy=False), k_eff)
    d = np.sqrt(np.maximum(d2, 0.0)).astype(np.float32, copy=False)

    valid = idx >= 0
    count = np.maximum(valid.sum(axis=1), 1)
    summed = (d * valid).sum(axis=1)
    out = summed / count
    out[count == 0] = 1.0
    return out.astype(np.float32, copy=False)


def _build_faiss_index(bank: np.ndarray, index_type: str, hnsw_m: int, hnsw_ef_search: int) -> tuple[object, str]:
    if not FAISS_AVAILABLE:
        raise RuntimeError("FAISS is not available")
    if bank.ndim != 2 or bank.shape[0] == 0:
        raise ValueError("Cannot build FAISS index for empty bank")

    n, d = int(bank.shape[0]), int(bank.shape[1])
    chosen = index_type
    if index_type == "auto":
        chosen = "hnsw" if n >= 400_000 else "flat"

    if chosen == "hnsw":
        index = faiss.IndexHNSWFlat(d, int(hnsw_m), faiss.METRIC_L2)
        index.hnsw.efSearch = int(hnsw_ef_search)
    else:
        index = faiss.IndexFlatL2(d)

    index.add(bank.astype(np.float32, copy=False))
    return index, chosen


def _haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    r = 6371.0
    phi1 = np.radians(lat1)
    phi2 = np.radians(lat2)
    d_phi = np.radians(lat2 - lat1)
    d_lambda = np.radians(lon2 - lon1)

    a = np.sin(d_phi / 2) ** 2 + np.cos(phi1) * np.cos(phi2) * np.sin(d_lambda / 2) ** 2
    c = 2 * np.arctan2(np.sqrt(a), np.sqrt(1 - a))
    return float(r * c)


def _angle_diff_deg(a: float, b: float) -> float:
    return float((b - a + 180.0) % 360.0 - 180.0)


def _seconds_between(prev_iso: str, curr_iso: str) -> Optional[float]:
    if not prev_iso or not curr_iso:
        return None
    try:
        prev = datetime.fromisoformat(str(prev_iso).replace("Z", "+00:00"))
        curr = datetime.fromisoformat(str(curr_iso).replace("Z", "+00:00"))
    except ValueError:
        return None
    return max((curr - prev).total_seconds(), 0.0)


def _time_of_day_encoding(iso_ts: str) -> Tuple[float, float]:
    try:
        ts = datetime.fromisoformat(str(iso_ts).replace("Z", "+00:00"))
    except ValueError:
        return 0.0, 1.0

    hour = ts.hour + ts.minute / 60.0 + ts.second / 3600.0
    phase = 2.0 * np.pi * hour / 24.0
    return float(np.sin(phase)), float(np.cos(phase))


def _pad(seq: np.ndarray, max_len: int) -> Tuple[np.ndarray, np.ndarray]:
    n, d = seq.shape
    x = np.zeros((max_len, d), dtype=np.float32)
    m = np.zeros((max_len,), dtype=np.float32)
    k = min(n, max_len)
    x[:k] = seq[:k]
    m[:k] = 1.0
    return x, m


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

        # Preserve vessel_type if available (used for fallback cohort scoring metadata).
        if "vessel_type" not in work.columns:
            work["vessel_type"] = "Unknown"

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
        vessel_type = str(matched.get("vessel_type", "Unknown"))

        label = "anomalous" if bool(matched.get("is_anomaly", score >= 0.8)) else "normal"
        return ModelScore(
            score=score,
            label=label,
            metadata={
                "matched_start_timestamp": str(matched.get("start_timestamp")),
                "vessel_type": vessel_type,
                "delta_sec": best_delta,
            },
        )


class AISRealtimeMemoryBankModel:
    """Realtime scorer that encodes the live voyage window and compares it to the memory bank."""

    def __init__(
        self,
        model_name: str,
        checkpoint_path: str,
        memory_dir: str,
        score_threshold: float = 0.8,
        k_neighbors: int = 5,
        min_window_points: int = 4,
        use_faiss: bool = True,
    ):
        self.model_name = model_name
        self.checkpoint_path = Path(checkpoint_path)
        self.memory_dir = Path(memory_dir)
        self.score_threshold = float(score_threshold)
        self.k_neighbors = max(1, int(k_neighbors))
        self.min_window_points = max(2, int(min_window_points))
        self.use_faiss = bool(use_faiss and FAISS_AVAILABLE)

        self._device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
        self._encoder: Optional[SequenceTransformerEncoder] = None
        self._max_len: Optional[int] = None
        self._global_bank: Optional[np.ndarray] = None
        self._global_index: Optional[object] = None
        self._grid_index: Dict[str, list[int]] = {}
        self._vessel_stats: Dict[str, Dict[str, Any]] = {}
        self._vessel_type_index: Dict[str, list[int]] = {}

        self._load_artifacts()

    def _load_artifacts(self) -> None:
        if not self.checkpoint_path.exists():
            raise FileNotFoundError(f"Checkpoint not found: {self.checkpoint_path}")
        if not self.memory_dir.exists():
            raise FileNotFoundError(f"Memory directory not found: {self.memory_dir}")

        ckpt = torch.load(self.checkpoint_path, map_location=self._device)
        self._encoder = SequenceTransformerEncoder(
            input_dim=int(ckpt["input_dim"]),
            model_dim=int(ckpt["model_dim"]),
            nhead=int(ckpt["nhead"]),
            layers=int(ckpt["layers"]),
            emb_dim=int(ckpt["emb_dim"]),
            max_pos_len=max(int(ckpt.get("max_len", 256)), int(ckpt.get("window_size", 30)), 512),
        ).to(self._device)
        self._encoder.load_state_dict(ckpt["state_dict"], strict=False)
        self._encoder.eval()
        self._max_len = int(ckpt["max_len"])

        global_bank = np.load(self.memory_dir / "global_embeddings.npy").astype(np.float32, copy=False)
        self._global_bank = _l2_normalize(global_bank)

        grid_path = self.memory_dir / "grid_memory_index.json"
        if grid_path.exists():
            with open(grid_path, "r", encoding="utf-8") as f:
                self._grid_index = {str(k): list(v) for k, v in json.load(f).items()}

        stats_path = self.memory_dir / "vessel_stats.json"
        if stats_path.exists():
            with open(stats_path, "r", encoding="utf-8") as f:
                for item in json.load(f):
                    vessel_id = str(item.get("mmsi"))
                    self._vessel_stats[vessel_id] = item

        metadata_path = self.memory_dir / "memory_metadata.parquet"
        if metadata_path.exists():
            memory_metadata = pd.read_parquet(metadata_path)
            if "vessel_type" not in memory_metadata.columns:
                memory_metadata["vessel_type"] = "Unknown"
            for idx, row in memory_metadata.reset_index(drop=True).iterrows():
                vessel_type = str(row.get("vessel_type", "Unknown")).strip() or "Unknown"
                self._vessel_type_index.setdefault(vessel_type, []).append(int(idx))

        if self.use_faiss:
            self._global_index, _ = _build_faiss_index(self._global_bank, index_type="auto", hnsw_m=32, hnsw_ef_search=64)

    def infer(self, feature_event: Dict[str, Any]) -> ModelScore:
        if self._encoder is None or self._max_len is None or self._global_bank is None:
            return ModelScore(score=0.0, label="normal", metadata={"reason": "model_not_loaded"})

        features = feature_event.get("features", {}) if isinstance(feature_event.get("features"), dict) else {}
        sequence = self._build_live_sequence(features)
        if sequence.shape[0] < self.min_window_points:
            return ModelScore(score=0.0, label="normal", metadata={"reason": "insufficient_window", "window_points": int(sequence.shape[0])})

        query = self._encode_sequence(sequence)
        global_score = self._knn_score(self._global_bank, query)

        start_lat, start_lon = self._extract_start_lat_lon(features)
        local_score = 1.0
        if start_lat is not None and start_lon is not None:
            cell_key = self._cell_key(start_lat, start_lon)
            local_idx = self._grid_index.get(cell_key, [])
            if local_idx:
                local_bank = self._global_bank[np.asarray(local_idx, dtype=int)]
                local_score = self._knn_score(local_bank, query)

        vessel_id = str(feature_event.get("vessel_id", ""))
        vessel_type = str(features.get("vessel_type") or feature_event.get("vessel_type") or "Unknown").strip() or "Unknown"

        vessel_score = 0.0
        if vessel_id in self._vessel_stats:
            vessel_score = self._vessel_baseline_score(vessel_id, query)
        elif vessel_type in self._vessel_type_index:
            cohort_idx = self._vessel_type_index[vessel_type]
            if cohort_idx:
                cohort_bank = self._global_bank[np.asarray(cohort_idx, dtype=int)]
                vessel_score = self._knn_score(cohort_bank, query)

        physics_score = self._physics_penalty(sequence)
        combined = 0.2 * physics_score + 0.4 * global_score + 0.2 * local_score + 0.2 * vessel_score
        label = "anomalous" if combined >= self.score_threshold else "normal"

        return ModelScore(
            score=float(min(max(combined, 0.0), 1.0)),
            label=label,
            metadata={
                "model_mode": "realtime_memory_bank",
                "global_score": float(global_score),
                "local_score": float(local_score),
                "vessel_score": float(vessel_score),
                "physics_score": float(physics_score),
                "vessel_type": vessel_type,
                "window_points": int(sequence.shape[0]),
            },
        )

    def _encode_sequence(self, sequence: np.ndarray) -> np.ndarray:
        assert self._encoder is not None and self._max_len is not None
        x, m = _pad(sequence, self._max_len)
        xt = torch.from_numpy(x).unsqueeze(0).to(self._device)
        mt = torch.from_numpy(m).unsqueeze(0).to(self._device)
        with torch.no_grad():
            emb = self._encoder(xt, mt).cpu().numpy()[0]
        return _l2_normalize(emb.reshape(1, -1))[0]

    def _knn_score(self, bank: np.ndarray, query: np.ndarray) -> float:
        if self.use_faiss and self._global_index is not None and bank is self._global_bank:
            return float(_faiss_mean_knn_distance(self._global_index, query.reshape(1, -1), self.k_neighbors)[0])
        return float(_numpy_mean_knn_distance(bank, query.reshape(1, -1), self.k_neighbors)[0])

    def _build_live_sequence(self, features: Dict[str, Any]) -> np.ndarray:
        trajectory = features.get("trajectory_window") or []
        timestamps = features.get("timestamp_window") or []
        speeds = features.get("speed_window_knots") or []
        headings = features.get("heading_window_deg") or []
        cogs = features.get("cog_window_deg") or []
        length_m = float(features.get("length_m") or 1.0)

        points = min(len(trajectory), len(timestamps))
        if points == 0:
            return np.empty((0, 10), dtype=np.float32)

        rows: list[list[float]] = []
        for idx in range(points):
            curr = trajectory[idx] or {}
            curr_lat = float(curr.get("lat", np.nan)) if isinstance(curr, dict) else np.nan
            curr_lon = float(curr.get("lon", np.nan)) if isinstance(curr, dict) else np.nan
            curr_ts = str(timestamps[idx])
            curr_speed = float(speeds[idx]) if idx < len(speeds) and speeds[idx] is not None else 0.0
            curr_heading = float(headings[idx]) if idx < len(headings) and headings[idx] is not None else 0.0
            curr_cog = float(cogs[idx]) if idx < len(cogs) and cogs[idx] is not None else curr_heading

            if idx == 0:
                distance_km = 0.0
                accel = 0.0
                turn_rate = 0.0
            else:
                prev = trajectory[idx - 1] or {}
                prev_lat = float(prev.get("lat", np.nan)) if isinstance(prev, dict) else np.nan
                prev_lon = float(prev.get("lon", np.nan)) if isinstance(prev, dict) else np.nan
                prev_ts = str(timestamps[idx - 1])
                prev_speed = float(speeds[idx - 1]) if idx - 1 < len(speeds) and speeds[idx - 1] is not None else curr_speed
                prev_cog = float(cogs[idx - 1]) if idx - 1 < len(cogs) and cogs[idx - 1] is not None else curr_cog
                distance_km = _haversine_km(prev_lat, prev_lon, curr_lat, curr_lon) if np.isfinite(prev_lat) and np.isfinite(prev_lon) and np.isfinite(curr_lat) and np.isfinite(curr_lon) else 0.0
                dt_sec = _seconds_between(prev_ts, curr_ts) or 0.0
                accel = (curr_speed - prev_speed) / dt_sec if dt_sec > 0 else 0.0
                turn_rate = _angle_diff_deg(prev_cog, curr_cog) / dt_sec if dt_sec > 0 else 0.0

            time_of_day_sin, time_of_day_cos = _time_of_day_encoding(curr_ts)
            heading_cog_diff = _angle_diff_deg(curr_heading, curr_cog)
            norm_speed = curr_speed / max(length_m, 1.0)

            rows.append(
                [
                    curr_speed,
                    accel,
                    turn_rate,
                    heading_cog_diff,
                    norm_speed,
                    time_of_day_sin,
                    time_of_day_cos,
                    np.sin(np.radians(curr_cog)),
                    np.cos(np.radians(curr_cog)),
                    distance_km,
                ]
            )

        return np.asarray(rows, dtype=np.float32)

    def _extract_start_lat_lon(self, features: Dict[str, Any]) -> Tuple[Optional[float], Optional[float]]:
        trajectory = features.get("trajectory_window") or []
        if not trajectory:
            return None, None
        first = trajectory[0] or {}
        if not isinstance(first, dict):
            return None, None
        try:
            return float(first.get("lat")), float(first.get("lon"))
        except (TypeError, ValueError):
            return None, None

    def _cell_key(self, lat: float, lon: float) -> str:
        return f"{int(np.floor(lat))}_{int(np.floor(lon))}"

    def _vessel_baseline_score(self, vessel_id: str, query: np.ndarray) -> float:
        stats = self._vessel_stats[vessel_id]
        mean = np.asarray(stats.get("mean", []), dtype=np.float32)
        var = np.asarray(stats.get("cov_diag", []), dtype=np.float32)
        if mean.size == 0 or var.size == 0:
            return 1.0
        var = np.clip(var, 1e-6, None)
        z = (query - mean) / np.sqrt(var)
        return float(np.sqrt((z * z).mean()))

    def _physics_penalty(self, sequence: np.ndarray) -> float:
        penalty = 0.0
        if sequence.shape[0] < self.min_window_points:
            penalty += 0.6
        # roughly 30 minutes of coverage is a decent minimum for a voyage window
        if sequence.shape[0] < 6:
            penalty += 0.4
        return float(min(penalty, 1.0))


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
