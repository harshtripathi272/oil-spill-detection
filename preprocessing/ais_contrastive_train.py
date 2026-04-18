from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path
from typing import List, Tuple

import numpy as np
import torch
import torch.nn as nn
import torch.nn.functional as F
from torch.utils.data import DataLoader, Dataset


def _load_npz_sequences(path: Path) -> List[np.ndarray]:
    data = np.load(path, allow_pickle=True)
    seqs = data["sequences"].tolist()
    return [np.asarray(s, dtype=np.float32) for s in seqs]


def _random_subsample(seq: np.ndarray, min_ratio: float = 0.7) -> np.ndarray:
    n = seq.shape[0]
    if n < 4:
        return seq.copy()
    k = max(2, int(n * np.random.uniform(min_ratio, 1.0)))
    idx = np.linspace(0, n - 1, k).astype(int)
    return seq[idx]


def _add_noise(seq: np.ndarray, std: float = 0.01) -> np.ndarray:
    return seq + np.random.normal(0.0, std, size=seq.shape).astype(np.float32)


def _time_warp(seq: np.ndarray, max_scale: float = 0.15) -> np.ndarray:
    n = seq.shape[0]
    if n < 4:
        return seq.copy()
    scale = 1.0 + np.random.uniform(-max_scale, max_scale)
    tgt_n = max(2, int(n * scale))
    src = np.linspace(0, n - 1, n)
    tgt = np.linspace(0, n - 1, tgt_n)
    warped = np.vstack([np.interp(tgt, src, seq[:, i]) for i in range(seq.shape[1])]).T
    return warped.astype(np.float32)


def _course_perturb(seq: np.ndarray, cog_sin_idx: int = 7, cog_cos_idx: int = 8, deg_std: float = 5.0) -> np.ndarray:
    out = seq.copy()
    if out.shape[1] <= max(cog_sin_idx, cog_cos_idx):
        return out
    angle = np.arctan2(out[:, cog_sin_idx], out[:, cog_cos_idx])
    angle = angle + np.radians(np.random.normal(0.0, deg_std, size=angle.shape[0]))
    out[:, cog_sin_idx] = np.sin(angle)
    out[:, cog_cos_idx] = np.cos(angle)
    return out


def augment_sequence(seq: np.ndarray) -> np.ndarray:
    out = _random_subsample(seq)
    out = _time_warp(out)
    out = _add_noise(out)
    out = _course_perturb(out)
    return out


class VoyageContrastiveDataset(Dataset):
    def __init__(self, sequences: List[np.ndarray], max_len: int):
        self.sequences = sequences
        self.max_len = max_len

    def __len__(self) -> int:
        return len(self.sequences)

    def _pad(self, seq: np.ndarray) -> Tuple[np.ndarray, np.ndarray]:
        n, d = seq.shape
        out = np.zeros((self.max_len, d), dtype=np.float32)
        mask = np.zeros((self.max_len,), dtype=np.float32)
        k = min(n, self.max_len)
        out[:k] = seq[:k]
        mask[:k] = 1.0
        return out, mask

    def __getitem__(self, idx: int):
        base = self.sequences[idx]
        v1 = augment_sequence(base)
        v2 = augment_sequence(base)
        x1, m1 = self._pad(v1)
        x2, m2 = self._pad(v2)
        return torch.from_numpy(x1), torch.from_numpy(m1), torch.from_numpy(x2), torch.from_numpy(m2)


class SequenceTransformerEncoder(nn.Module):
    def __init__(self, input_dim: int, model_dim: int, nhead: int, layers: int, emb_dim: int):
        super().__init__()
        self.proj = nn.Linear(input_dim, model_dim)
        enc_layer = nn.TransformerEncoderLayer(d_model=model_dim, nhead=nhead, batch_first=True)
        self.encoder = nn.TransformerEncoder(enc_layer, num_layers=layers)
        self.head = nn.Sequential(
            nn.Linear(model_dim, model_dim),
            nn.ReLU(),
            nn.Linear(model_dim, emb_dim),
        )

    def forward(self, x: torch.Tensor, mask: torch.Tensor) -> torch.Tensor:
        h = self.proj(x)
        key_padding_mask = mask < 0.5
        h = self.encoder(h, src_key_padding_mask=key_padding_mask)
        denom = torch.clamp(mask.sum(dim=1, keepdim=True), min=1.0)
        pooled = (h * mask.unsqueeze(-1)).sum(dim=1) / denom
        z = self.head(pooled)
        return F.normalize(z, dim=-1)


def info_nce(z1: torch.Tensor, z2: torch.Tensor, temperature: float = 0.1) -> torch.Tensor:
    batch = z1.size(0)
    z = torch.cat([z1, z2], dim=0)
    sim = z @ z.T / temperature

    mask = torch.eye(2 * batch, device=z.device, dtype=torch.bool)
    sim = sim.masked_fill(mask, -1e9)

    pos = torch.cat([
        torch.arange(batch, 2 * batch, device=z.device),
        torch.arange(0, batch, device=z.device),
    ])
    return F.cross_entropy(sim, pos)


@dataclass
class TrainConfig:
    epochs: int = 10
    batch_size: int = 64
    lr: float = 1e-3
    max_len: int = 256
    model_dim: int = 128
    nhead: int = 4
    layers: int = 2
    emb_dim: int = 64


def train(sequences_path: Path, output_dir: Path, cfg: TrainConfig) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)

    seqs = _load_npz_sequences(sequences_path)
    if not seqs:
        raise ValueError("No sequences found for training")

    input_dim = int(seqs[0].shape[1])
    ds = VoyageContrastiveDataset(seqs, max_len=cfg.max_len)
    dl = DataLoader(ds, batch_size=cfg.batch_size, shuffle=True, drop_last=True)

    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    model = SequenceTransformerEncoder(
        input_dim=input_dim,
        model_dim=cfg.model_dim,
        nhead=cfg.nhead,
        layers=cfg.layers,
        emb_dim=cfg.emb_dim,
    ).to(device)

    opt = torch.optim.Adam(model.parameters(), lr=cfg.lr)

    for epoch in range(cfg.epochs):
        model.train()
        losses: List[float] = []
        for x1, m1, x2, m2 in dl:
            x1, m1 = x1.to(device), m1.to(device)
            x2, m2 = x2.to(device), m2.to(device)

            z1 = model(x1, m1)
            z2 = model(x2, m2)
            loss = info_nce(z1, z2)

            opt.zero_grad(set_to_none=True)
            loss.backward()
            opt.step()
            losses.append(float(loss.item()))

        print(f"Epoch {epoch + 1}/{cfg.epochs} loss={np.mean(losses):.4f}")

    ckpt = {
        "state_dict": model.state_dict(),
        "input_dim": input_dim,
        "model_dim": cfg.model_dim,
        "nhead": cfg.nhead,
        "layers": cfg.layers,
        "emb_dim": cfg.emb_dim,
        "max_len": cfg.max_len,
    }
    torch.save(ckpt, output_dir / "encoder.pt")
    print(f"Saved encoder checkpoint to {output_dir / 'encoder.pt'}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Train contrastive AIS sequence encoder")
    parser.add_argument("--sequences-path", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--epochs", type=int, default=10)
    parser.add_argument("--batch-size", type=int, default=64)
    parser.add_argument("--lr", type=float, default=1e-3)
    parser.add_argument("--max-len", type=int, default=256)
    parser.add_argument("--model-dim", type=int, default=128)
    parser.add_argument("--nhead", type=int, default=4)
    parser.add_argument("--layers", type=int, default=2)
    parser.add_argument("--emb-dim", type=int, default=64)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    cfg = TrainConfig(
        epochs=args.epochs,
        batch_size=args.batch_size,
        lr=args.lr,
        max_len=args.max_len,
        model_dim=args.model_dim,
        nhead=args.nhead,
        layers=args.layers,
        emb_dim=args.emb_dim,
    )
    train(args.sequences_path, args.output_dir, cfg)


if __name__ == "__main__":
    main()
