from __future__ import annotations

import argparse
import importlib
from pathlib import Path
from typing import Optional, Tuple

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import torch
from sklearn.manifold import TSNE
from sklearn.decomposition import PCA
from tqdm import tqdm

from preprocessing.ais_contrastive_train import SequenceTransformerEncoder
from preprocessing.ais_preprocessing import SEQUENCE_FEATURE_COLUMNS


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


def _load_sequences(sequences_path: Path) -> list[np.ndarray]:
    if not sequences_path.exists():
        raise FileNotFoundError(f"Sequences file not found: {sequences_path}")
    payload = np.load(sequences_path, allow_pickle=True)
    return [np.asarray(seq, dtype=np.float32) for seq in payload["sequences"].tolist()]


def _load_embeddings(embeddings_path: Path) -> np.ndarray:
    if not embeddings_path.exists():
        raise FileNotFoundError(f"Embeddings file not found: {embeddings_path}")
    emb = np.load(embeddings_path)
    emb = np.asarray(emb, dtype=np.float32)
    if emb.ndim != 2:
        raise ValueError(f"Expected 2D embeddings array, got shape {emb.shape}")
    return emb


def _encode_sequences(model: SequenceTransformerEncoder, max_len: int, sequences: list[np.ndarray], device: torch.device) -> np.ndarray:
    embeddings = []
    with torch.no_grad():
        for seq in tqdm(sequences, desc="Encoding sequences", unit="voyage"):
            x, m = _pad(seq, max_len)
            xt = torch.from_numpy(x).unsqueeze(0).to(device)
            mt = torch.from_numpy(m).unsqueeze(0).to(device)
            z = model(xt, mt).cpu().numpy()[0]
            embeddings.append(z)
    return np.asarray(embeddings, dtype=np.float32)


def _reduce_embeddings(embeddings: np.ndarray, method: str, perplexity: float, random_state: int) -> np.ndarray:
    if method == "umap":
        umap_spec = importlib.util.find_spec("umap")
        if umap_spec is None:
            raise ImportError("umap-learn is not installed. Install it or use --method tsne.")
        umap = importlib.import_module("umap")
        reducer = umap.UMAP(n_components=2, random_state=random_state)
        return reducer.fit_transform(embeddings)

    if method == "pca":
        reducer = PCA(n_components=2, random_state=random_state)
        return reducer.fit_transform(embeddings)

    max_perplexity = max(5.0, min(perplexity, (len(embeddings) - 1) / 3.0))
    reducer = TSNE(n_components=2, perplexity=max_perplexity, random_state=random_state, init="pca", learning_rate="auto")
    return reducer.fit_transform(embeddings)


def _load_labels(metadata_path: Optional[Path], scores_path: Optional[Path], label_column: str) -> Tuple[Optional[np.ndarray], str]:
    if scores_path is not None:
        if not scores_path.exists():
            raise FileNotFoundError(f"Scores file not found: {scores_path}")
        scores = pd.read_parquet(scores_path)
        if "combined_score" in scores.columns:
            return scores["combined_score"].to_numpy(), "continuous"
        numeric_cols = [c for c in scores.columns if pd.api.types.is_numeric_dtype(scores[c])]
        if not numeric_cols:
            raise ValueError(f"No numeric score columns found in {scores_path}")
        return scores[numeric_cols[0]].to_numpy(), "continuous"

    if metadata_path is None:
        return None, "none"
    if not metadata_path.exists():
        raise FileNotFoundError(f"Metadata file not found: {metadata_path}")
    metadata = pd.read_parquet(metadata_path)
    if label_column not in metadata.columns:
        raise ValueError(f"Label column '{label_column}' not found in {metadata_path}")
    labels = metadata[label_column]
    return labels.to_numpy(), "auto"


def plot_embeddings(embeddings_2d: np.ndarray, labels: Optional[np.ndarray], output_file: Path, title: str) -> None:
    output_file.parent.mkdir(parents=True, exist_ok=True)
    plt.figure(figsize=(10, 8))

    if labels is None:
        plt.scatter(embeddings_2d[:, 0], embeddings_2d[:, 1], s=8, alpha=0.75)
    else:
        labels_arr = np.asarray(labels)
        if np.issubdtype(labels_arr.dtype, np.number) and len(np.unique(labels_arr)) > 20:
            scatter = plt.scatter(embeddings_2d[:, 0], embeddings_2d[:, 1], c=labels_arr, cmap="coolwarm", s=8, alpha=0.8)
            plt.colorbar(scatter, label="Label / score")
        else:
            encoded, uniques = pd.factorize(labels_arr.astype(str))
            scatter = plt.scatter(embeddings_2d[:, 0], embeddings_2d[:, 1], c=encoded, cmap="tab20", s=8, alpha=0.8)
            handles = []
            for idx, name in enumerate(uniques[:20]):
                handles.append(plt.Line2D([], [], marker="o", linestyle="", markersize=6, label=str(name), markerfacecolor=plt.cm.tab20(idx % 20), markeredgecolor="none"))
            if handles:
                plt.legend(handles=handles, loc="best", fontsize=8, frameon=False)

    plt.title(title)
    plt.xlabel("Dim 1")
    plt.ylabel("Dim 2")
    plt.tight_layout()
    plt.savefig(output_file, dpi=200)
    plt.close()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Visualize AIS embeddings with t-SNE or UMAP")
    parser.add_argument("--embeddings-path", type=Path, default=None)
    parser.add_argument("--sequences-path", type=Path, default=None)
    parser.add_argument("--checkpoint", type=Path, default=None)
    parser.add_argument("--metadata-path", type=Path, default=None)
    parser.add_argument("--scores-path", type=Path, default=None)
    parser.add_argument("--label-column", type=str, default="vessel_type")
    parser.add_argument("--method", type=str, choices=["tsne", "pca", "umap"], default="tsne")
    parser.add_argument("--perplexity", type=float, default=30.0)
    parser.add_argument("--max-points", type=int, default=5000)
    parser.add_argument("--sample-seed", type=int, default=42)
    parser.add_argument("--output-file", type=Path, default=Path("preprocessing/outputs/ais_visuals/embeddings.png"))
    parser.add_argument("--save-embeddings", type=Path, default=None)
    parser.add_argument("--title", type=str, default="AIS Embedding Space")
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    selected_idx: Optional[np.ndarray] = None
    if args.embeddings_path is not None:
        embeddings = _load_embeddings(args.embeddings_path)
    else:
        if args.sequences_path is None or args.checkpoint is None:
            raise ValueError("Provide --embeddings-path, or provide both --sequences-path and --checkpoint.")

        sequences = _load_sequences(args.sequences_path)
        if not sequences:
            raise ValueError(f"No sequences found in {args.sequences_path}")

        device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
        model, max_len = _load_model(args.checkpoint, device=device)
        embeddings = _encode_sequences(model, max_len=max_len, sequences=sequences, device=device)

    if args.save_embeddings is not None:
        args.save_embeddings.parent.mkdir(parents=True, exist_ok=True)
        np.save(args.save_embeddings, embeddings)

    if len(embeddings) > args.max_points:
        rng = np.random.default_rng(args.sample_seed)
        selected_idx = np.sort(rng.choice(len(embeddings), size=args.max_points, replace=False))
        embeddings = embeddings[selected_idx]

    labels, _ = _load_labels(args.metadata_path, args.scores_path, args.label_column)
    if labels is not None and selected_idx is not None:
        labels = labels[selected_idx]
    if labels is not None and len(labels) != len(embeddings):
        raise ValueError(f"Label count ({len(labels)}) does not match embeddings count ({len(embeddings)})")

    emb_2d = _reduce_embeddings(embeddings, method=args.method, perplexity=args.perplexity, random_state=42)
    plot_embeddings(emb_2d, labels, output_file=args.output_file, title=args.title)
    print(f"Saved embedding plot to {args.output_file}")


if __name__ == "__main__":
    main()