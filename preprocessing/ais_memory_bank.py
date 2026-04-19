from __future__ import annotations

import argparse
import json
from glob import glob
from pathlib import Path
from typing import Dict, List, Tuple

import numpy as np
import pandas as pd
import torch
from tqdm import tqdm

from preprocessing.ais_contrastive_train import SequenceTransformerEncoder


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


def _pad_seq(seq: np.ndarray, max_len: int) -> Tuple[np.ndarray, np.ndarray]:
    n, d = seq.shape
    x = np.zeros((max_len, d), dtype=np.float32)
    m = np.zeros((max_len,), dtype=np.float32)
    k = min(n, max_len)
    x[:k] = seq[:k]
    m[:k] = 1.0
    return x, m


def _normalize_mmsi_series(series: pd.Series) -> pd.Series:
    """Normalize MMSI values so keys match across parquet sources.

    Handles common formatting drift like numeric floats rendered as strings
    (e.g., "123456789.0"), scientific notation, and blank/null values.
    """
    raw = series.astype(str).str.strip()
    num = pd.to_numeric(raw, errors="coerce")
    out = raw.copy()

    valid = num.notna()
    if valid.any():
        out.loc[valid] = num.loc[valid].round().astype("int64").astype(str)

    out = out.replace({"": "nan", "None": "nan", "<NA>": "nan"})
    return out


def _build_mmsi_to_vessel_type_map(input_glob: str) -> Dict[str, str]:
    paths = sorted(glob(input_glob, recursive=True))
    if not paths:
        raise FileNotFoundError(f"No AIS parquet files matched: {input_glob}")

    pairs: List[pd.DataFrame] = []
    for path in tqdm(paths, desc="Loading vessel_type map", unit="file"):
        try:
            df = pd.read_parquet(path, columns=["mmsi", "vessel_type"])
        except Exception:
            # Some shards may not include vessel_type; skip them.
            continue

        if "mmsi" not in df.columns or "vessel_type" not in df.columns:
            continue

        d = df[["mmsi", "vessel_type"]].copy()
        d["mmsi"] = _normalize_mmsi_series(d["mmsi"])
        d["vessel_type"] = d["vessel_type"].astype(str).str.strip()
        d = d[(d["mmsi"].str.lower() != "nan") & (d["vessel_type"] != "") & (d["vessel_type"].str.lower() != "nan")]
        if not d.empty:
            pairs.append(d)

    if not pairs:
        return {}

    all_pairs = pd.concat(pairs, ignore_index=True)
    # Choose the most frequent vessel_type for each MMSI.
    counts = all_pairs.groupby(["mmsi", "vessel_type"], dropna=False).size().reset_index(name="n")
    best = counts.sort_values(["mmsi", "n"], ascending=[True, False]).drop_duplicates(["mmsi"], keep="first")
    return {str(r["mmsi"]): str(r["vessel_type"]) for _, r in best.iterrows()}


def _enrich_metadata_with_vessel_type(metadata: pd.DataFrame, input_glob: str | None) -> pd.DataFrame:
    out = metadata.copy()
    if "mmsi" not in out.columns:
        raise ValueError("Metadata is missing required 'mmsi' column")

    out["mmsi"] = _normalize_mmsi_series(out["mmsi"])
    if "vessel_type" not in out.columns:
        out["vessel_type"] = "Unknown"
    else:
        out["vessel_type"] = out["vessel_type"].astype(str).str.strip().replace({"": "Unknown", "nan": "Unknown", "None": "Unknown"})

    if not input_glob:
        return out

    mmsi_to_type = _build_mmsi_to_vessel_type_map(input_glob)
    if not mmsi_to_type:
        return out

    mapped = out["mmsi"].map(mmsi_to_type)
    needs_fill = out["vessel_type"].isin(["Unknown", "nan", "None", ""]) | out["vessel_type"].isna()
    out.loc[needs_fill, "vessel_type"] = mapped[needs_fill].fillna(out.loc[needs_fill, "vessel_type"])
    out["vessel_type"] = out["vessel_type"].fillna("Unknown").astype(str).str.strip()
    out.loc[out["vessel_type"] == "", "vessel_type"] = "Unknown"
    return out


def build_memory_bank(
    sequences_path: Path,
    metadata_path: Path,
    checkpoint: Path,
    output_dir: Path,
    grid_deg: float = 1.0,
    input_glob: str | None = None,
    update_metadata_inplace: bool = True,
) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)

    if not sequences_path.exists():
        raise FileNotFoundError(f"Sequences file not found: {sequences_path}")
    if not metadata_path.exists():
        raise FileNotFoundError(f"Metadata file not found: {metadata_path}")

    payload = np.load(sequences_path, allow_pickle=True)
    sequences = payload["sequences"].tolist()
    metadata = pd.read_parquet(metadata_path)
    metadata = _enrich_metadata_with_vessel_type(metadata, input_glob=input_glob)
    if update_metadata_inplace:
        metadata.to_parquet(metadata_path, index=False)
        print(f"Updated metadata with vessel_type at: {metadata_path}")

    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    model, max_len = _load_model(checkpoint, device=device)

    embeds: List[np.ndarray] = []
    for seq in tqdm(sequences, desc="Encoding memory bank", unit="voyage"):
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
    for i, row in tqdm(metadata.reset_index(drop=True).iterrows(), total=len(metadata), desc="Building grid index", unit="voyage"):
        lat_bin = int(np.floor(float(row["start_lat"]) / grid_deg))
        lon_bin = int(np.floor(float(row["start_lon"]) / grid_deg))
        key = f"{lat_bin}_{lon_bin}"
        grid_map.setdefault(key, []).append(i)

    with open(output_dir / "grid_memory_index.json", "w", encoding="utf-8") as f:
        json.dump(grid_map, f)

    # Lightweight per-vessel running stats (MMSI-specific).
    vessel_stats = []
    md = metadata.reset_index(drop=True)
    for mmsi, grp in tqdm(md.groupby("mmsi"), total=int(md["mmsi"].nunique()), desc="Building vessel stats", unit="vessel"):
        idx = grp.index.to_numpy()
        if len(idx) < 2:
            continue
        v_emb = emb[idx]
        mean = v_emb.mean(axis=0)
        cov_diag = v_emb.var(axis=0)
        vessel_type_val = grp["vessel_type"].iloc[0]
        vessel_type_str = str(vessel_type_val).strip() if pd.notna(vessel_type_val) else "Unknown"
        vessel_stats.append(
            {
                "mmsi": str(mmsi),
                "vessel_type": vessel_type_str,
                "count": int(len(idx)),
                "mean": mean.tolist(),
                "cov_diag": cov_diag.tolist(),
            }
        )

    with open(output_dir / "vessel_stats.json", "w", encoding="utf-8") as f:
        json.dump(vessel_stats, f)

    # Build vessel-type cohort index (fallback for unknown vessels).
    vessel_type_index: Dict[str, List[int]] = {}
    for i, row in md.iterrows():
        vtype = str(row.get("vessel_type", "Unknown")).strip()
        if not vtype or vtype == "Unknown":
            vtype = "Unknown"
        vessel_type_index.setdefault(vtype, []).append(i)

    with open(output_dir / "vessel_type_index.json", "w", encoding="utf-8") as f:
        json.dump(vessel_type_index, f)

    metadata.to_parquet(output_dir / "memory_metadata.parquet", index=False)
    print(f"Saved memory bank artifacts to {output_dir}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build AIS embedding memory bank")
    parser.add_argument("--sequences-path", type=Path, required=True)
    parser.add_argument("--metadata-path", type=Path, required=True)
    parser.add_argument("--checkpoint", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--grid-deg", type=float, default=1.0)
    parser.add_argument(
        "--input-glob",
        type=str,
        default=None,
        help="Optional AIS parquet glob used to map MMSI to vessel_type and enrich voyage metadata",
    )
    parser.add_argument(
        "--no-update-metadata-inplace",
        action="store_true",
        help="Do not write enriched vessel_type values back to --metadata-path",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    build_memory_bank(
        sequences_path=args.sequences_path,
        metadata_path=args.metadata_path,
        checkpoint=args.checkpoint,
        output_dir=args.output_dir,
        grid_deg=args.grid_deg,
        input_glob=args.input_glob,
        update_metadata_inplace=not args.no_update_metadata_inplace,
    )


if __name__ == "__main__":
    main()
