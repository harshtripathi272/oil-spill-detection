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

try:
    import faiss

    FAISS_AVAILABLE = True
except ImportError:
    faiss = None
    FAISS_AVAILABLE = False

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
    embedding_batch_size: int = 256
    use_faiss: bool = True
    faiss_index_type: str = "auto"
    faiss_hnsw_m: int = 32
    faiss_hnsw_ef_search: int = 64
    faiss_write_index: bool = False
    faiss_index_path: Path | None = None


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


def _l2_normalize(arr: np.ndarray, eps: float = 1e-12) -> np.ndarray:
    arr = np.asarray(arr, dtype=np.float32)
    norms = np.linalg.norm(arr, axis=1, keepdims=True)
    norms = np.maximum(norms, eps)
    return arr / norms


def _encode_sequences(
    model: SequenceTransformerEncoder,
    max_len: int,
    sequences: List[np.ndarray],
    device: torch.device,
    batch_size: int,
) -> np.ndarray:
    out: list[np.ndarray] = []
    with torch.no_grad():
        total = len(sequences)
        for start in tqdm(range(0, total, max(1, batch_size)), desc="Encoding inference sequences", unit="batch"):
            chunk = sequences[start : start + max(1, batch_size)]
            xs, ms = [], []
            for seq in chunk:
                x, m = _pad(np.asarray(seq, dtype=np.float32), max_len)
                xs.append(x)
                ms.append(m)
            xt = torch.from_numpy(np.stack(xs, axis=0)).to(device)
            mt = torch.from_numpy(np.stack(ms, axis=0)).to(device)
            z = model(xt, mt).cpu().numpy()
            out.append(np.asarray(z, dtype=np.float32))
    if not out:
        return np.empty((0, 0), dtype=np.float32)
    return np.concatenate(out, axis=0)


def _build_faiss_index(
    bank: np.ndarray,
    index_type: str,
    hnsw_m: int,
    hnsw_ef_search: int,
) -> tuple[object, str]:
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


def _numpy_mean_knn_distance(bank: np.ndarray, queries: np.ndarray, k: int) -> np.ndarray:
    if queries.size == 0:
        return np.empty((0,), dtype=np.float32)
    if bank.size == 0:
        return np.ones((queries.shape[0],), dtype=np.float32)

    # Embeddings are normalized, so L2 distance can be computed from cosine similarity.
    sims = queries @ bank.T
    k_eff = max(1, min(int(k), int(bank.shape[0])))
    topk = np.partition(sims, kth=sims.shape[1] - k_eff, axis=1)[:, -k_eff:]
    d = np.sqrt(np.maximum(2.0 - 2.0 * topk, 0.0)).astype(np.float32, copy=False)
    return d.mean(axis=1).astype(np.float32, copy=False)


def _physics_penalty(meta_row: pd.Series) -> float:
    # Lightweight rule score; intended to catch low-level quality issues.
    penalty = 0.0
    dur = float(meta_row.get("duration_sec", 0.0))
    if dur < 15 * 60:
        penalty += 0.4
    if float(meta_row.get("n_steps", 0)) < 4:
        penalty += 0.6
    return min(penalty, 1.0)


def _vessel_type_cohort_score(
    query_embedding: np.ndarray,
    global_bank: np.ndarray,
    vessel_type_indices: Dict[str, List[int]],
    vessel_type: str,
    k: int = 5,
) -> float:
    """
    Compute anomaly score for unknown vessel using type-based cohort.
    Returns mean KNN distance to other vessels of same type.
    """
    vtype_clean = str(vessel_type).strip() if pd.notna(vessel_type) else "Unknown"
    cohort_idx = vessel_type_indices.get(vtype_clean, [])
    
    if not cohort_idx:
        return 1.0  # fallback: no cohort found
    
    cohort_bank = global_bank[np.asarray(cohort_idx, dtype=int)]
    q_norm = query_embedding.reshape(1, -1)
    sims = q_norm @ cohort_bank.T
    k_eff = max(1, min(int(k), int(cohort_bank.shape[0])))
    topk = np.partition(sims, kth=sims.shape[1] - k_eff, axis=1)[:, -k_eff:]
    d = np.sqrt(np.maximum(2.0 - 2.0 * topk, 0.0)).astype(np.float32, copy=False)
    return float(d.mean())



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
    global_bank = np.load(memory_dir / "global_embeddings.npy").astype(np.float32, copy=False)
    global_bank = _l2_normalize(global_bank)
    with open(memory_dir / "grid_memory_index.json", "r", encoding="utf-8") as f:
        grid_index = json.load(f)

    vessel_stats: Dict[str, Dict[str, object]] = {}
    vs_path = memory_dir / "vessel_stats.json"
    if vs_path.exists():
        with open(vs_path, "r", encoding="utf-8") as f:
            for item in json.load(f):
                vessel_stats[str(item["mmsi"])] = item

    vessel_type_index: Dict[str, List[int]] = {}
    vt_path = memory_dir / "vessel_type_index.json"
    if vt_path.exists():
        with open(vt_path, "r", encoding="utf-8") as f:
            vessel_type_index = json.load(f)

    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    model, max_len = _load_model(checkpoint, device=device)
    emb = _encode_sequences(
        model,
        max_len=max_len,
        sequences=sequences,
        device=device,
        batch_size=max(1, cfg.embedding_batch_size),
    )
    emb = _l2_normalize(emb)

    use_faiss = cfg.use_faiss and FAISS_AVAILABLE
    if cfg.use_faiss and not FAISS_AVAILABLE:
        print("FAISS unavailable; falling back to NumPy KNN search.")

    global_index = None
    if use_faiss:
        index_path = cfg.faiss_index_path
        if index_path is None and cfg.faiss_write_index:
            index_path = memory_dir / "global_embeddings.faiss"

        if index_path is not None and index_path.exists():
            try:
                global_index = faiss.read_index(str(index_path))
                if int(global_index.ntotal) != int(global_bank.shape[0]):
                    print("Stored FAISS index size mismatch. Rebuilding index from embeddings.")
                    global_index = None
            except Exception as exc:
                print(f"Failed reading FAISS index from disk ({index_path}): {exc}. Rebuilding.")
                global_index = None

        if global_index is None:
            global_index, chosen = _build_faiss_index(
                global_bank,
                index_type=cfg.faiss_index_type,
                hnsw_m=cfg.faiss_hnsw_m,
                hnsw_ef_search=cfg.faiss_hnsw_ef_search,
            )
            print(f"Built global FAISS index: type={chosen}, vectors={global_bank.shape[0]}")
            if cfg.faiss_write_index:
                persist_path = index_path if index_path is not None else (memory_dir / "global_embeddings.faiss")
                faiss.write_index(global_index, str(persist_path))
                print(f"Saved FAISS index to: {persist_path}")

    if use_faiss and global_index is not None:
        global_scores = _faiss_mean_knn_distance(global_index, emb, cfg.k_neighbors)
    else:
        global_scores = _numpy_mean_knn_distance(global_bank, emb, cfg.k_neighbors)

    query_cell_keys: list[str] = []
    cell_to_query_idx: dict[str, list[int]] = {}
    for i, row in metadata.iterrows():
        lat_bin = int(np.floor(float(row["start_lat"])))
        lon_bin = int(np.floor(float(row["start_lon"])))
        key = f"{lat_bin}_{lon_bin}"
        query_cell_keys.append(key)
        cell_to_query_idx.setdefault(key, []).append(i)

    local_scores = np.ones((len(metadata),), dtype=np.float32)
    local_index_cache: dict[str, object] = {}
    local_bank_cache: dict[str, np.ndarray] = {}
    for key, query_indices in tqdm(cell_to_query_idx.items(), desc="Scoring local memory", unit="cell"):
        local_idx = grid_index.get(key, [])
        if not local_idx:
            continue

        local_bank = global_bank[np.asarray(local_idx, dtype=int)]
        q_batch = emb[np.asarray(query_indices, dtype=int)]

        if use_faiss and global_index is not None:
            if key not in local_index_cache:
                local_index_cache[key], _ = _build_faiss_index(
                    local_bank,
                    index_type=cfg.faiss_index_type,
                    hnsw_m=cfg.faiss_hnsw_m,
                    hnsw_ef_search=cfg.faiss_hnsw_ef_search,
                )
            d = _faiss_mean_knn_distance(local_index_cache[key], q_batch, cfg.k_neighbors)
        else:
            if key not in local_bank_cache:
                local_bank_cache[key] = local_bank
            d = _numpy_mean_knn_distance(local_bank_cache[key], q_batch, cfg.k_neighbors)

        local_scores[np.asarray(query_indices, dtype=int)] = d

    rows = []
    for i, row in metadata.iterrows():
        q = emb[i]

        physics_score = _physics_penalty(row)
        global_score = float(global_scores[i])
        local_score = float(local_scores[i])

        vessel_id = str(row["mmsi"])
        vessel_type = str(row.get("vessel_type", "Unknown")).strip()
        
        # Primary: MMSI-specific vessel score (for known vessels with history).
        vessel_score = 0.0
        if vessel_id in vessel_stats:
            stats = vessel_stats[vessel_id]
            if int(stats.get("count", 0)) >= 3:
                mean = np.asarray(stats.get("mean", []), dtype=np.float32)
                var = np.asarray(stats.get("cov_diag", []), dtype=np.float32)
                var = np.clip(var, 1e-6, None)
                z = (q - mean) / np.sqrt(var)
                vessel_score = float(np.sqrt((z * z).mean()))
        else:
            # Fallback: vessel-type cohort score (for unknown vessels, use type-based anomaly detection).
            if vessel_type_index:
                vessel_score = _vessel_type_cohort_score(q, global_bank, vessel_type_index, vessel_type, k=cfg.k_neighbors)

        # Combined hierarchical score.
        combined = 0.2 * physics_score + 0.4 * global_score + 0.2 * local_score + 0.2 * vessel_score

        rows.append(
            {
                "voyage_id": row["voyage_id"],
                "mmsi": vessel_id,
                "vessel_type": vessel_type,
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
    parser.add_argument("--embedding-batch-size", type=int, default=256)
    parser.add_argument(
        "--use-faiss",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Use FAISS for KNN search when available. Falls back to NumPy if unavailable.",
    )
    parser.add_argument(
        "--faiss-index-type",
        type=str,
        choices=["auto", "flat", "hnsw"],
        default="auto",
        help="FAISS index type for global/local memory search.",
    )
    parser.add_argument("--faiss-hnsw-m", type=int, default=32)
    parser.add_argument("--faiss-hnsw-ef-search", type=int, default=64)
    parser.add_argument(
        "--faiss-write-index",
        action="store_true",
        help="Persist global FAISS index to disk for faster startup.",
    )
    parser.add_argument(
        "--faiss-index-path",
        type=Path,
        default=None,
        help="Optional path to load/save global FAISS index.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    cfg = InferenceConfig(
        burn_in_hours=args.burn_in_hours,
        score_threshold=args.score_threshold,
        k_neighbors=args.k_neighbors,
        embedding_batch_size=args.embedding_batch_size,
        use_faiss=args.use_faiss,
        faiss_index_type=args.faiss_index_type,
        faiss_hnsw_m=args.faiss_hnsw_m,
        faiss_hnsw_ef_search=args.faiss_hnsw_ef_search,
        faiss_write_index=args.faiss_write_index,
        faiss_index_path=args.faiss_index_path,
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
