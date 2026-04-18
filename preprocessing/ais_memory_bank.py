from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Dict, List, Tuple

import numpy as np
import pandas as pd
import torch

from preprocessing.ais_contrastive_train import SequenceTransformerEncoder


def _load_model(checkpoint: Path, device: torch.device) -> Tuple[SequenceTransformerEncoder, int]:
    ckpt = torch.load(checkpoint, map_location=device)
    model = SequenceTransformerEncoder(
        input_dim=int(ckpt["input_dim"]),
        model_dim=int(ckpt["model_dim"]),
        nhead=int(ckpt["nhead"]),
        layers=int(ckpt["layers"]),
        emb_dim=int(ckpt["emb_dim"]),
    ).to(device)
    model.load_state_dict(ckpt["state_dict"])
    model.eval()
    return model, int(ckpt["max_len"])


def _pad_seq(seq: np.ndarray, max_len: int) -> Tuple[np.ndarray, np.ndarray]:
    n, d = seq.shape
    x = np.zeros((max_len, d), dtype=np.float32)
    m = np.zeros((max_len,), dtype=np.float32)
    k = min(n, max_len)
    x[:k] = seq[:k]
    m[:k] = 1.0
    return x, m


def build_memory_bank(sequences_path: Path, metadata_path: Path, checkpoint: Path, output_dir: Path, grid_deg: float = 1.0) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)

    payload = np.load(sequences_path, allow_pickle=True)
    sequences = payload["sequences"].tolist()
    metadata = pd.read_parquet(metadata_path)

    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    model, max_len = _load_model(checkpoint, device=device)

    embeds: List[np.ndarray] = []
    for seq in sequences:
        x, m = _pad_seq(np.asarray(seq, dtype=np.float32), max_len=max_len)
        xt = torch.from_numpy(x).unsqueeze(0).to(device)
        mt = torch.from_numpy(m).unsqueeze(0).to(device)
        with torch.no_grad():
            z = model(xt, mt).cpu().numpy()[0]
        embeds.append(z)

    emb = np.asarray(embeds, dtype=np.float32)
    np.save(output_dir / "global_embeddings.npy", emb)

    # Build location-specific subsets using voyage start lat/lon.
    grid_map: Dict[str, List[int]] = {}
    for i, row in metadata.reset_index(drop=True).iterrows():
        lat_bin = int(np.floor(float(row["start_lat"]) / grid_deg))
        lon_bin = int(np.floor(float(row["start_lon"]) / grid_deg))
        key = f"{lat_bin}_{lon_bin}"
        grid_map.setdefault(key, []).append(i)

    with open(output_dir / "grid_memory_index.json", "w", encoding="utf-8") as f:
        json.dump(grid_map, f)

    # Lightweight per-vessel running stats.
    vessel_stats = []
    md = metadata.reset_index(drop=True)
    for mmsi, grp in md.groupby("mmsi"):
        idx = grp.index.to_numpy()
        if len(idx) < 2:
            continue
        v_emb = emb[idx]
        mean = v_emb.mean(axis=0)
        cov_diag = v_emb.var(axis=0)
        vessel_stats.append(
            {
                "mmsi": str(mmsi),
                "count": int(len(idx)),
                "mean": mean.tolist(),
                "cov_diag": cov_diag.tolist(),
            }
        )

    with open(output_dir / "vessel_stats.json", "w", encoding="utf-8") as f:
        json.dump(vessel_stats, f)

    metadata.to_parquet(output_dir / "memory_metadata.parquet", index=False)
    print(f"Saved memory bank artifacts to {output_dir}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build AIS embedding memory bank")
    parser.add_argument("--sequences-path", type=Path, required=True)
    parser.add_argument("--metadata-path", type=Path, required=True)
    parser.add_argument("--checkpoint", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--grid-deg", type=float, default=1.0)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    build_memory_bank(
        sequences_path=args.sequences_path,
        metadata_path=args.metadata_path,
        checkpoint=args.checkpoint,
        output_dir=args.output_dir,
        grid_deg=args.grid_deg,
    )


if __name__ == "__main__":
    main()
