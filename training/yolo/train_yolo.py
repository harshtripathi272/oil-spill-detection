from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import cv2
import numpy as np
import wandb
from ultralytics import YOLO

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from training.common.evaluator import evaluate_split
from training.common.seed import seed_everything


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Train YOLO11 segmentation with W&B logging.")
    parser.add_argument("--data", type=Path, default=ROOT / "datasets/dataset.yaml")
    parser.add_argument("--model", type=str, default="yolo11n-seg.pt")
    parser.add_argument("--epochs", type=int, default=50)
    parser.add_argument("--imgsz", type=int, default=640)
    parser.add_argument("--batch", type=int, default=8)
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--lr0", type=float, default=1e-3)
    parser.add_argument("--weight-decay", type=float, default=5e-4)
    parser.add_argument("--optimizer", type=str, default="AdamW")
    parser.add_argument("--device", type=str, default="0")
    parser.add_argument("--project-dir", type=Path, default=ROOT / "runs/yolo")
    parser.add_argument("--run-name", type=str, default="yolo11n-seg-baseline")
    parser.add_argument("--wandb-project", type=str, default="oilspill")
    parser.add_argument("--wandb-entity", type=str, default=None)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--conf-thres", type=float, default=0.25)
    parser.add_argument("--save-json", type=Path, default=None)
    return parser.parse_args()


def _predictor_for_model(model: YOLO, imgsz: int, conf: float, device: str):
    def _predict(image_path: Path, orig_shape: tuple[int, int]) -> np.ndarray:
        height, width = orig_shape
        result = model.predict(
            source=str(image_path),
            imgsz=imgsz,
            conf=conf,
            device=device,
            verbose=False,
        )[0]
        if result.masks is None or result.masks.data is None:
            return np.zeros((height, width), dtype=np.uint8)

        masks = result.masks.data.detach().cpu().numpy()
        combined = (masks > 0.5).any(axis=0).astype(np.uint8)
        if combined.shape != (height, width):
            combined = cv2.resize(combined, (width, height), interpolation=cv2.INTER_NEAREST)
        return combined

    return _predict


def run_training(config: dict[str, object]) -> dict[str, float]:
    seed_everything(int(config["seed"]))

    created_run = False
    wandb_run = wandb.run
    if wandb_run is None:
        wandb_run = wandb.init(
            project=str(config["wandb_project"]),
            entity=config.get("wandb_entity"),
            name=str(config["run_name"]),
            config=config,
            reinit=True,
        )
        created_run = True

    model = YOLO(str(config["model"]))
    model.train(
        data=str(config["data"]),
        epochs=int(config["epochs"]),
        imgsz=int(config["imgsz"]),
        batch=int(config["batch"]),
        workers=int(config["workers"]),
        project=str(config["project_dir"]),
        name=str(config["run_name"]),
        optimizer=str(config["optimizer"]),
        lr0=float(config["lr0"]),
        weight_decay=float(config["weight_decay"]),
        device=str(config["device"]),
        seed=int(config["seed"]),
        verbose=True,
    )

    trained_run_dir = Path(config["project_dir"]) / str(config["run_name"])
    best_weights = trained_run_dir / "weights/best.pt"
    if best_weights.exists():
        model = YOLO(str(best_weights))

    data_parent = Path(config["data"]).parent
    val_images = data_parent / "images/val"
    val_labels = data_parent / "labels/val"

    predictor = _predictor_for_model(
        model=model,
        imgsz=int(config["imgsz"]),
        conf=float(config["conf_thres"]),
        device=str(config["device"]),
    )
    metrics = evaluate_split(val_images, val_labels, predictor, verbose=True)

    wandb.log({f"val/{k}": v for k, v in metrics.items()})
    if best_weights.exists():
        artifact = wandb.Artifact(name=f"{wandb_run.name}-best", type="model")
        artifact.add_file(str(best_weights))
        wandb.log_artifact(artifact)

    if created_run:
        wandb.finish()
    return metrics


def main() -> None:
    args = parse_args()
    config = {
        "data": args.data,
        "model": args.model,
        "epochs": args.epochs,
        "imgsz": args.imgsz,
        "batch": args.batch,
        "workers": args.workers,
        "lr0": args.lr0,
        "weight_decay": args.weight_decay,
        "optimizer": args.optimizer,
        "device": args.device,
        "project_dir": args.project_dir,
        "run_name": args.run_name,
        "wandb_project": args.wandb_project,
        "wandb_entity": args.wandb_entity,
        "seed": args.seed,
        "conf_thres": args.conf_thres,
    }
    metrics = run_training(config)
    print(json.dumps(metrics, indent=2))

    if args.save_json:
        args.save_json.parent.mkdir(parents=True, exist_ok=True)
        args.save_json.write_text(json.dumps(metrics, indent=2), encoding="utf-8")


if __name__ == "__main__":
    main()
