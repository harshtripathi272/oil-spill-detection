from __future__ import annotations

import os
import sys
from pathlib import Path

import wandb

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from training.yolo.train_yolo import run_training


def main() -> None:
    with wandb.init(project=os.getenv("WANDB_PROJECT", "oilspill")):
        cfg = dict(wandb.config)
        cfg.setdefault("data", ROOT / "datasets/dataset.yaml")
        cfg.setdefault("model", "yolo11n-seg.pt")
        cfg.setdefault("epochs", 50)
        cfg.setdefault("imgsz", 640)
        cfg.setdefault("batch", 8)
        cfg.setdefault("workers", 4)
        cfg.setdefault("lr0", 1e-3)
        cfg.setdefault("weight_decay", 5e-4)
        cfg.setdefault("optimizer", "AdamW")
        cfg.setdefault("device", "0")
        cfg.setdefault("project_dir", ROOT / "runs/yolo")
        cfg.setdefault("run_name", f"sweep-{wandb.run.id}")
        cfg.setdefault("wandb_project", os.getenv("WANDB_PROJECT", "oilspill"))
        cfg.setdefault("wandb_entity", os.getenv("WANDB_ENTITY"))
        cfg.setdefault("seed", 42)
        cfg.setdefault("conf_thres", 0.25)

        run_training(cfg)


if __name__ == "__main__":
    main()
