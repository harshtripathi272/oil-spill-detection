from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Tuple

import numpy as np
import pandas as pd
import torch
from tqdm import tqdm

from preprocessing.ais_contrastive_train import SequenceTransformerEncoder
from preprocessing.ais_preprocessing import (
    AISPreprocessConfig,
    SEQUENCE_FEATURE_COLUMNS,
    build_sequence_dataset,
    clean_ais_globally,
    compute_physics_features,
    load_ais_parquets,
    resample_voyages_kinematic,
    segment_voyages,
)


@dataclass
class InferenceConfig:
    burn_in_hours: float = 24.0
    score_threshold: float = 0.8
    k_neighbors: int = 5


def _load_model(checkpoint: Path, device: torch.device) -> Tuple[SequenceTransformerEncoder, int]:
    if not checkpoint.exists():
        raise FileNotFoundError(f"Checkpoint not found: {checkpoint}")
    ckpt = torch.load(checkpoint, map_location=device)
    model = SequenceTransformerEncoder(
        input_dim=int(ckpt["input_dim"]),
        model_dim=int(ckpt["model_dim"]),
        nhead=int(ckpt["nhead"]),
        layers=int(ckpt["layers"]),
        emb_dim=int(ckpt["emb_dim"]),
        max_pos_len=max(int(ckpt.get("max_len", 256)), int(ckpt.get("window_size", 30)), 512),
    ).to(device)
    model.load_state_dict(ckpt["state_dict"], strict=False)
    model.eval()
    return model, int(ckpt["max_len"])


def _pad(seq: np.ndarray, max_len: int) -> Tuple[np.ndarray, np.ndarray]:
    n, d = seq.shape
    x = np.zeros((max_len, d), dtype=np.float32)
    m = np.zeros((max_len,), dtype=np.float32)
    k = min(n, max_len)
    x[:k] = seq[:k]
    m[:k] = 1.0
    return x, m


def _encode_sequences(model: SequenceTransformerEncoder, max_len: int, sequences: List[np.ndarray], device: torch.device) -> np.ndarray:
    out = []
    with torch.no_grad():
        for seq in tqdm(sequences, desc="Encoding inference sequences", unit="voyage"):
            x, m = _pad(np.asarray(seq, dtype=np.float32), max_len)
            xt = torch.from_numpy(x).unsqueeze(0).to(device)
            mt = torch.from_numpy(m).unsqueeze(0).to(device)
            z = model(xt, mt).cpu().numpy()[0]
            out.append(z)
    return np.asarray(out, dtype=np.float32)


def _knn_distance(query: np.ndarray, bank: np.ndarray, k: int) -> float:
    if bank.size == 0:
        return 1.0
    d = np.linalg.norm(bank - query[None, :], axis=1)
    k = max(1, min(k, len(d)))
    return float(np.partition(d, k - 1)[:k].mean())


def _physics_penalty(meta_row: pd.Series) -> float:
    # Lightweight rule score; intended to catch low-level quality issues.
    penalty = 0.0
    dur = float(meta_row.get("duration_sec", 0.0))
    if dur < 15 * 60:
        penalty += 0.4
    if float(meta_row.get("n_steps", 0)) < 4:
        penalty += 0.6
    return min(penalty, 1.0)


def run_inference(
    input_glob: str,
    checkpoint: Path,
    memory_dir: Path,
    output_file: Path,
    cfg: InferenceConfig,
) -> None:
    tmp_dir = output_file.parent / "_tmp_inference_sequences"
    tmp_dir.mkdir(parents=True, exist_ok=True)

    preprocess_cfg = AISPreprocessConfig()
    raw = load_ais_parquets(input_glob=input_glob)
    clean = clean_ais_globally(raw, cfg=preprocess_cfg)
    voyages = segment_voyages(clean, cfg=preprocess_cfg)
    resampled = resample_voyages_kinematic(voyages, interval=preprocess_cfg.resample_interval)
    feat = compute_physics_features(resampled)
    build_sequence_dataset(feat, output_dir=tmp_dir)

    payload = np.load(tmp_dir / "voyage_sequences.npz", allow_pickle=True)
    sequences = payload["sequences"].tolist()
    metadata = pd.read_parquet(tmp_dir / "voyage_metadata.parquet").reset_index(drop=True)

    if not memory_dir.exists():
        raise FileNotFoundError(f"Memory directory not found: {memory_dir}")
    global_bank = np.load(memory_dir / "global_embeddings.npy")
    with open(memory_dir / "grid_memory_index.json", "r", encoding="utf-8") as f:
        grid_index = json.load(f)

    vessel_stats: Dict[str, Dict[str, object]] = {}
    vs_path = memory_dir / "vessel_stats.json"
    if vs_path.exists():
        with open(vs_path, "r", encoding="utf-8") as f:
            for item in json.load(f):
                vessel_stats[str(item["mmsi"])] = item

    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    model, max_len = _load_model(checkpoint, device=device)
    emb = _encode_sequences(model, max_len=max_len, sequences=sequences, device=device)

    rows = []
    for i, row in metadata.iterrows():
        q = emb[i]

        physics_score = _physics_penalty(row)
        global_score = _knn_distance(q, global_bank, cfg.k_neighbors)

        lat_bin = int(np.floor(float(row["start_lat"])))
        lon_bin = int(np.floor(float(row["start_lon"])))
        key = f"{lat_bin}_{lon_bin}"
        local_idx = grid_index.get(key, [])
        local_bank = global_bank[np.asarray(local_idx, dtype=int)] if local_idx else np.empty((0, global_bank.shape[1]), dtype=np.float32)
        local_score = _knn_distance(q, local_bank, cfg.k_neighbors)

        vessel_id = str(row["mmsi"])
        vessel_score = 0.0
        if vessel_id in vessel_stats:
            stats = vessel_stats[vessel_id]
            if int(stats.get("count", 0)) >= 3:
                mean = np.asarray(stats.get("mean", []), dtype=np.float32)
                var = np.asarray(stats.get("cov_diag", []), dtype=np.float32)
                var = np.clip(var, 1e-6, None)
                z = (q - mean) / np.sqrt(var)
                vessel_score = float(np.sqrt((z * z).mean()))

        # Combined hierarchical score.
        combined = 0.2 * physics_score + 0.4 * global_score + 0.2 * local_score + 0.2 * vessel_score

        rows.append(
            {
                "voyage_id": row["voyage_id"],
                "mmsi": vessel_id,
                "start_timestamp": row["start_timestamp"],
                "duration_sec": float(row["duration_sec"]),
                "physics_score": physics_score,
                "global_score": global_score,
                "local_score": local_score,
                "vessel_score": vessel_score,
                "combined_score": combined,
                "is_anomaly": combined >= cfg.score_threshold,
            }
        )

    out = pd.DataFrame(rows)
    output_file.parent.mkdir(parents=True, exist_ok=True)
    out.to_parquet(output_file, index=False)
    print(f"Saved inference scores to {output_file}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="AIS hierarchical anomaly inference")
    parser.add_argument("--input-glob", type=str, required=True)
    parser.add_argument("--checkpoint", type=Path, required=True)
    parser.add_argument("--memory-dir", type=Path, required=True)
    parser.add_argument("--output-file", type=Path, required=True)
    parser.add_argument("--burn-in-hours", type=float, default=24.0)
    parser.add_argument("--score-threshold", type=float, default=0.8)
    parser.add_argument("--k-neighbors", type=int, default=5)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    cfg = InferenceConfig(
        burn_in_hours=args.burn_in_hours,
        score_threshold=args.score_threshold,
        k_neighbors=args.k_neighbors,
    )
    run_inference(
        input_glob=args.input_glob,
        checkpoint=args.checkpoint,
        memory_dir=args.memory_dir,
        output_file=args.output_file,
        cfg=cfg,
    )


if __name__ == "__main__":
    main()
