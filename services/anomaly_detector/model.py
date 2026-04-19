import hashlib
import logging
import json
import time
from dataclasses import dataclass, field
from datetime import datetime
from collections import deque
from pathlib import Path
from typing import Any, Deque, Dict, Optional, Tuple

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

logger = logging.getLogger(__name__)

LIVE_FEATURE_COLUMNS = [
    "speed_knots",
    "accel_knots_per_sec",
    "turn_rate_deg_per_sec",
    "heading_cog_diff_deg",
    "norm_speed",
    "tod_sin",
    "tod_cos",
    "cog_sin",
    "cog_cos",
    "distance_km",
]


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
        trajectory_window_size: int = 30,
        score_smoothing_window_size: int = 5,
        use_faiss: bool = True,
    ):
        self.model_name = model_name
        self.checkpoint_path = Path(checkpoint_path)
        self.memory_dir = Path(memory_dir)
        self.score_threshold = float(score_threshold)
        self.k_neighbors = max(1, int(k_neighbors))
        self.min_window_points = max(2, int(min_window_points))
        self.trajectory_window_size = max(2, int(trajectory_window_size))
        self.score_smoothing_window_size = max(1, int(score_smoothing_window_size))
        self.use_faiss = bool(use_faiss and FAISS_AVAILABLE)

        self._device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
        self._encoder: Optional[SequenceTransformerEncoder] = None
        self._max_len: Optional[int] = None
        self._global_bank: Optional[np.ndarray] = None
        self._global_index: Optional[object] = None
        self._grid_index: Dict[str, list[int]] = {}
        self._vessel_stats: Dict[str, Dict[str, Any]] = {}
        self._vessel_type_index: Dict[str, list[int]] = {}
        self._trajectory_buffers: Dict[str, Deque[Dict[str, Any]]] = {}
        self._score_buffers: Dict[str, Deque[float]] = {}
        self._local_faiss_cache: Dict[str, object] = {}
        self._cohort_faiss_cache: Dict[str, object] = {}
        self._encode_latency_total_sec = 0.0
        self._encode_latency_count = 0

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

        vessel_id = str(feature_event.get("vessel_id", "")).strip()
        if not vessel_id:
            return ModelScore(score=0.0, label="insufficient_data", metadata={"reason": "missing_vessel_id"})

        current_observation = self._extract_current_observation(feature_event)
        if current_observation is None:
            return ModelScore(score=0.0, label="insufficient_data", metadata={"reason": "missing_current_observation"})

        self._append_observation(vessel_id, current_observation)
        sequence = self._build_live_sequence(vessel_id)
        if sequence.shape[0] < self.min_window_points:
            return ModelScore(
                score=0.0,
                label="insufficient_data",
                metadata={"reason": "warmup_insufficient_window", "window_points": int(sequence.shape[0])},
            )

        query = self._encode_sequence(sequence)
        global_score = self._knn_score(self._global_bank, query, self._global_index)

        start_lat, start_lon = self._extract_start_lat_lon_from_sequence(vessel_id)
        local_score = 1.0
        if start_lat is not None and start_lon is not None:
            cell_key = self._cell_key(start_lat, start_lon)
            local_idx = self._grid_index.get(cell_key, [])
            if local_idx:
                local_bank = self._global_bank[np.asarray(local_idx, dtype=int)]
                local_index = self._local_faiss_cache.get(cell_key)
                if self.use_faiss and local_index is None:
                    local_index, _ = _build_faiss_index(local_bank, index_type="auto", hnsw_m=32, hnsw_ef_search=64)
                    self._local_faiss_cache[cell_key] = local_index
                local_score = self._knn_score(local_bank, query, local_index)

        vessel_type = str(current_observation.get("vessel_type") or feature_event.get("vessel_type") or "Unknown").strip() or "Unknown"

        vessel_score = 0.0
        if vessel_id in self._vessel_stats:
            vessel_score = self._vessel_baseline_score(vessel_id, query)
        elif vessel_type in self._vessel_type_index:
            cohort_idx = self._vessel_type_index[vessel_type]
            if cohort_idx:
                cohort_bank = self._global_bank[np.asarray(cohort_idx, dtype=int)]
                cohort_index = self._cohort_faiss_cache.get(vessel_type)
                if self.use_faiss and cohort_index is None:
                    cohort_index, _ = _build_faiss_index(cohort_bank, index_type="auto", hnsw_m=32, hnsw_ef_search=64)
                    self._cohort_faiss_cache[vessel_type] = cohort_index
                vessel_score = self._knn_score(cohort_bank, query, cohort_index)

        physics_score = self._physics_penalty(sequence)
        combined = 0.2 * physics_score + 0.4 * global_score + 0.2 * local_score + 0.2 * vessel_score

        score_buffer = self._get_score_buffer(vessel_id)
        score_buffer.append(float(combined))
        smoothed_score = float(np.mean(score_buffer))
        label = "anomalous" if smoothed_score >= self.score_threshold else "normal"

        return ModelScore(
            score=float(min(max(smoothed_score, 0.0), 1.0)),
            label=label,
            metadata={
                "model_mode": "realtime_memory_bank",
                "global_score": float(global_score),
                "local_score": float(local_score),
                "vessel_score": float(vessel_score),
                "physics_score": float(physics_score),
                "raw_combined_score": float(combined),
                "smoothed_score": float(smoothed_score),
                "vessel_type": vessel_type,
                "window_points": int(sequence.shape[0]),
                "score_buffer_len": int(len(score_buffer)),
            },
        )

    def _encode_sequence(self, sequence: np.ndarray) -> np.ndarray:
        assert self._encoder is not None and self._max_len is not None
        start = time.perf_counter()
        x, m = _pad(sequence, self._max_len)
        xt = torch.from_numpy(x).unsqueeze(0).to(self._device)
        mt = torch.from_numpy(m).unsqueeze(0).to(self._device)
        with torch.no_grad():
            emb = self._encoder(xt, mt).cpu().numpy()[0]
        if self._device.type == "cuda":
            torch.cuda.synchronize()
        elapsed = time.perf_counter() - start
        self._encode_latency_total_sec += elapsed
        self._encode_latency_count += 1
        if self._encode_latency_count % 500 == 0:
            avg_latency = self._encode_latency_total_sec / self._encode_latency_count
            logger.info(
                "realtime_encode_latency avg_sec=%.6f samples=%d device=%s",
                avg_latency,
                self._encode_latency_count,
                self._device.type,
            )
        return _l2_normalize(emb.reshape(1, -1))[0]

    def _knn_score(self, bank: np.ndarray, query: np.ndarray, faiss_index: object | None = None) -> float:
        if self.use_faiss and faiss_index is not None:
            return float(_faiss_mean_knn_distance(faiss_index, query.reshape(1, -1), self.k_neighbors)[0])
        return float(_numpy_mean_knn_distance(bank, query.reshape(1, -1), self.k_neighbors)[0])

    def _build_live_sequence(self, vessel_id: str) -> np.ndarray:
        buffer = self._trajectory_buffers.get(vessel_id)
        if not buffer or len(buffer) < 2:
            return np.empty((0, len(LIVE_FEATURE_COLUMNS)), dtype=np.float32)

        rows: list[list[float]] = []
        observations = list(buffer)
        for idx, current in enumerate(observations):
            curr_lat = float(current.get("lat", np.nan))
            curr_lon = float(current.get("lon", np.nan))
            curr_ts = str(current.get("timestamp", ""))
            curr_speed = float(current.get("speed_knots", 0.0) or 0.0)
            curr_heading = float(current.get("heading_deg", 0.0) or 0.0)
            curr_cog = float(current.get("cog_deg", curr_heading) or curr_heading)
            curr_length = float(current.get("length_m", 1.0) or 1.0)

            if idx == 0:
                distance_km = 0.0
                accel = 0.0
                turn_rate = 0.0
            else:
                prev = observations[idx - 1]
                prev_lat = float(prev.get("lat", np.nan))
                prev_lon = float(prev.get("lon", np.nan))
                prev_ts = str(prev.get("timestamp", ""))
                prev_speed = float(prev.get("speed_knots", curr_speed) or curr_speed)
                prev_cog = float(prev.get("cog_deg", curr_cog) or curr_cog)
                distance_km = _haversine_km(prev_lat, prev_lon, curr_lat, curr_lon) if np.isfinite(prev_lat) and np.isfinite(prev_lon) and np.isfinite(curr_lat) and np.isfinite(curr_lon) else 0.0
                dt_sec = _seconds_between(prev_ts, curr_ts) or 0.0
                accel = (curr_speed - prev_speed) / dt_sec if dt_sec > 0 else 0.0
                turn_rate = _angle_diff_deg(prev_cog, curr_cog) / dt_sec if dt_sec > 0 else 0.0

            tod_sin, tod_cos = _time_of_day_encoding(curr_ts)
            heading_cog_diff = _angle_diff_deg(curr_heading, curr_cog)
            norm_speed = curr_speed / max(curr_length, 1.0)

            rows.append(
                [
                    curr_speed,
                    accel,
                    turn_rate,
                    heading_cog_diff,
                    norm_speed,
                    tod_sin,
                    tod_cos,
                    np.sin(np.radians(curr_cog)),
                    np.cos(np.radians(curr_cog)),
                    distance_km,
                ]
            )

        sequence = np.asarray(rows, dtype=np.float32)
        if sequence.ndim != 2 or sequence.shape[1] != len(LIVE_FEATURE_COLUMNS):
            logger.warning(
                "live_feature_schema_mismatch vessel_id=%s actual_shape=%s expected_columns=%d",
                vessel_id,
                tuple(sequence.shape),
                len(LIVE_FEATURE_COLUMNS),
            )
            return np.empty((0, len(LIVE_FEATURE_COLUMNS)), dtype=np.float32)
        return sequence

    def _extract_start_lat_lon_from_sequence(self, vessel_id: str) -> Tuple[Optional[float], Optional[float]]:
        buffer = self._trajectory_buffers.get(vessel_id)
        if not buffer:
            return None, None
        first = next(iter(buffer), None)
        if not first:
            return None, None
        try:
            return float(first.get("lat")), float(first.get("lon"))
        except (TypeError, ValueError):
            return None, None

    def _get_trajectory_buffer(self, vessel_id: str) -> Deque[Dict[str, Any]]:
        buffer = self._trajectory_buffers.get(vessel_id)
        if buffer is None:
            buffer = deque(maxlen=self.trajectory_window_size)
            self._trajectory_buffers[vessel_id] = buffer
        return buffer

    def _get_score_buffer(self, vessel_id: str) -> Deque[float]:
        buffer = self._score_buffers.get(vessel_id)
        if buffer is None:
            buffer = deque(maxlen=self.score_smoothing_window_size)
            self._score_buffers[vessel_id] = buffer
        return buffer

    def _append_observation(self, vessel_id: str, observation: Dict[str, Any]) -> None:
        buffer = self._get_trajectory_buffer(vessel_id)
        buffer.append(observation)

    def _extract_current_observation(self, feature_event: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        features = feature_event.get("features", {}) if isinstance(feature_event.get("features"), dict) else {}

        lat = _to_float(feature_event.get("lat", features.get("lat")), None)
        lon = _to_float(feature_event.get("lon", features.get("lon")), None)
        timestamp = feature_event.get("timestamp") or features.get("timestamp")
        if lat is None or lon is None or timestamp is None:
            return None

        speed = _to_float(features.get("speed_knots", feature_event.get("speed_knots")), 0.0)
        heading = _to_float(features.get("heading_deg", feature_event.get("heading_deg")), 0.0)
        cog = _to_float(features.get("cog_deg", feature_event.get("cog_deg", heading)), heading)
        length_m = _to_float(features.get("length_m", feature_event.get("length_m")), 1.0)
        vessel_type = feature_event.get("vessel_type") or features.get("vessel_type") or "Unknown"

        return {
            "lat": float(lat),
            "lon": float(lon),
            "timestamp": str(timestamp),
            "speed_knots": float(speed if speed is not None else 0.0),
            "heading_deg": float(heading if heading is not None else 0.0),
            "cog_deg": float(cog if cog is not None else heading if heading is not None else 0.0),
            "length_m": float(length_m if length_m is not None else 1.0),
            "vessel_type": str(vessel_type),
        }

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
