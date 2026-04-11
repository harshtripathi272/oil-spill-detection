from __future__ import annotations

import os
import sys
from pathlib import Path

import wandb

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from training.yolo.train_yolo import run_training


def _get_sweep_metric(metrics: dict[str, float]) -> float:
    # Use the exact key your evaluator returns for mAP@50 if available.
    # Change/add keys here to match your evaluate_split output.
    for key in ("map50", "mAP50", "seg/map50", "mask/map50", "f1", "iou"):
        if key in metrics:
            return float(metrics[key])
    raise KeyError(
        f"No usable sweep metric found in metrics: {list(metrics.keys())}. "
        "Make sure your evaluator returns map50/mAP50 or update this mapping."
    )


def main() -> None:
    wandb.init(
        project=os.getenv("WANDB_PROJECT", "oilspill"),
        entity=os.getenv("WANDB_ENTITY"),
    )

    cfg = dict(wandb.config)

    defaults = {
        "data": ROOT / "datasets/dataset.yaml",
        "model": "yolo11x-seg.pt",   # better starting point for A100 than yolo11n
        "epochs": 60,
        "imgsz": 768,
        "batch": 16,
        "workers": 8,
        "lr0": 3e-4,
        "weight_decay": 5e-4,
        "optimizer": "AdamW",
        "device": "0",
        "project_dir": ROOT / "runs/yolo",
        "seed": 42,
        "conf_thres": 0.25,
        "momentum": 0.937,
        "warmup_epochs": 3,
        "mosaic": 1.0,
        "close_mosaic": 10,
        "hsv_h": 0.015,
        "hsv_s": 0.7,
        "hsv_v": 0.4,
        "degrees": 0.0,
        "scale": 0.5,
        "fliplr": 0.5,
    }

    for k, v in defaults.items():
        cfg.setdefault(k, v)

    cfg["data"] = str(cfg["data"])
    cfg["project_dir"] = str(cfg["project_dir"])
    cfg["run_name"] = f"sweep-{wandb.run.id}"
    cfg["wandb_project"] = os.getenv("WANDB_PROJECT", "oilspill")
    cfg["wandb_entity"] = os.getenv("WANDB_ENTITY")

    metrics = run_training(cfg)

    sweep_metric = _get_sweep_metric(metrics)
    wandb.log({"sweep_metric": sweep_metric})
    wandb.summary["sweep_metric"] = sweep_metric


if __name__ == "__main__":
    main()