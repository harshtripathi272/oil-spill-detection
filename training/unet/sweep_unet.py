from __future__ import annotations

import os
import sys
from pathlib import Path

import wandb

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from training.unet.train_unet import run_training


def main() -> None:
    with wandb.init(project=os.getenv("WANDB_PROJECT", "oilspill")):
        cfg = dict(wandb.config)
        cfg.setdefault("data_root", ROOT / "datasets")
        cfg.setdefault("encoder", "efficientnet-b0")
        cfg.setdefault("epochs", 40)
        cfg.setdefault("image_size", 512)
        cfg.setdefault("batch_size", 4)
        cfg.setdefault("num_workers", 4)
        cfg.setdefault("lr", 1e-3)
        cfg.setdefault("weight_decay", 1e-4)
        cfg.setdefault("dice_weight", 0.5)
        cfg.setdefault("threshold", 0.5)
        cfg.setdefault("device", "cuda")
        cfg.setdefault("seed", 42)
        cfg.setdefault("output_dir", ROOT / "runs/unet")
        cfg.setdefault("run_name", f"sweep-{wandb.run.id}")
        cfg.setdefault("wandb_project", os.getenv("WANDB_PROJECT", "oilspill"))
        cfg.setdefault("wandb_entity", os.getenv("WANDB_ENTITY"))

        run_training(cfg)


if __name__ == "__main__":
    main()
