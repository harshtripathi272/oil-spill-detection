from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import numpy as np
import torch
import torch.nn.functional as F
import wandb
from torch import nn
from torch.cuda.amp import GradScaler, autocast
from torch.utils.data import DataLoader
from tqdm import tqdm

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from training.common.evaluator import evaluate_split
from training.common.seed import seed_everything
from training.unet.dataset import OilSpillSegmentationDataset
from training.unet.model import build_unet


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Train UNet segmentation with W&B logging.")
    parser.add_argument("--data-root", type=Path, default=ROOT / "datasets")
    parser.add_argument("--encoder", type=str, default="efficientnet-b0")
    parser.add_argument("--epochs", type=int, default=40)
    parser.add_argument("--image-size", type=int, default=512)
    parser.add_argument("--batch-size", type=int, default=4)
    parser.add_argument("--num-workers", type=int, default=4)
    parser.add_argument("--lr", type=float, default=1e-3)
    parser.add_argument("--weight-decay", type=float, default=1e-4)
    parser.add_argument("--dice-weight", type=float, default=0.5)
    parser.add_argument("--threshold", type=float, default=0.5)
    parser.add_argument("--device", type=str, default="cuda")
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--output-dir", type=Path, default=ROOT / "runs/unet")
    parser.add_argument("--run-name", type=str, default="unet-efficientnetb0-baseline")
    parser.add_argument("--wandb-project", type=str, default="oilspill")
    parser.add_argument("--wandb-entity", type=str, default=None)
    parser.add_argument("--save-json", type=Path, default=None)
    return parser.parse_args()


def dice_loss(logits: torch.Tensor, targets: torch.Tensor, eps: float = 1e-6) -> torch.Tensor:
    probs = torch.sigmoid(logits)
    intersection = (probs * targets).sum(dim=(2, 3))
    union = probs.sum(dim=(2, 3)) + targets.sum(dim=(2, 3))
    dice = (2 * intersection + eps) / (union + eps)
    return 1.0 - dice.mean()


def train_one_epoch(
    model: nn.Module,
    loader: DataLoader,
    optimizer: torch.optim.Optimizer,
    scaler: GradScaler,
    device: torch.device,
    dice_weight: float,
) -> float:
    model.train()
    total_loss = 0.0

    pbar = tqdm(loader, desc="UNet Train", unit="batch")
    for images, masks in pbar:
        images = images.to(device, non_blocking=True)
        masks = masks.to(device, non_blocking=True)

        optimizer.zero_grad(set_to_none=True)
        with autocast(enabled=device.type == "cuda"):
            logits = model(images)
            bce = F.binary_cross_entropy_with_logits(logits, masks)
            dloss = dice_loss(logits, masks)
            loss = (1.0 - dice_weight) * bce + dice_weight * dloss

        scaler.scale(loss).backward()
        scaler.step(optimizer)
        scaler.update()

        total_loss += float(loss.item())
        pbar.set_postfix(loss=f"{loss.item():.4f}")

    return total_loss / max(1, len(loader))


def _unet_predictor(model: nn.Module, image_size: int, device: torch.device, threshold: float):
    def _predict(image_path: Path, orig_shape: tuple[int, int]) -> np.ndarray:
        import cv2

        image = cv2.imread(str(image_path), cv2.IMREAD_COLOR)
        image = cv2.cvtColor(image, cv2.COLOR_BGR2RGB)
        resized = cv2.resize(image, (image_size, image_size), interpolation=cv2.INTER_AREA)

        tensor = torch.from_numpy(resized).permute(2, 0, 1).float().unsqueeze(0) / 255.0
        tensor = tensor.to(device)

        model.eval()
        with torch.no_grad(), autocast(enabled=device.type == "cuda"):
            logits = model(tensor)
            probs = torch.sigmoid(logits)[0, 0].detach().cpu().numpy()

        mask = (probs >= threshold).astype(np.uint8)
        h, w = orig_shape
        if mask.shape != (h, w):
            mask = cv2.resize(mask, (w, h), interpolation=cv2.INTER_NEAREST)
        return mask

    return _predict


def validate(
    model: nn.Module,
    data_root: Path,
    image_size: int,
    device: torch.device,
    threshold: float,
) -> dict[str, float]:
    predictor = _unet_predictor(model, image_size=image_size, device=device, threshold=threshold)
    return evaluate_split(
        images_dir=data_root / "images/val",
        labels_dir=data_root / "labels/val",
        predictor=predictor,
        verbose=True,
    )


def run_training(config: dict[str, object]) -> dict[str, float]:
    seed_everything(int(config["seed"]))

    device = torch.device(
        str(config["device"]) if torch.cuda.is_available() else "cpu"
    )

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

    train_ds = OilSpillSegmentationDataset(
        root_dir=Path(config["data_root"]),
        split="train",
        image_size=int(config["image_size"]),
    )
    train_loader = DataLoader(
        train_ds,
        batch_size=int(config["batch_size"]),
        shuffle=True,
        num_workers=int(config["num_workers"]),
        pin_memory=True,
    )

    model = build_unet(encoder_name=str(config["encoder"]))
    model.to(device)

    optimizer = torch.optim.AdamW(
        model.parameters(),
        lr=float(config["lr"]),
        weight_decay=float(config["weight_decay"]),
    )
    scaler = GradScaler(enabled=device.type == "cuda")

    best_dice = -1.0
    output_dir = Path(config["output_dir"]) / str(config["run_name"])
    output_dir.mkdir(parents=True, exist_ok=True)
    best_path = output_dir / "best_unet.pt"

    for epoch in range(1, int(config["epochs"]) + 1):
        train_loss = train_one_epoch(
            model=model,
            loader=train_loader,
            optimizer=optimizer,
            scaler=scaler,
            device=device,
            dice_weight=float(config["dice_weight"]),
        )

        val_metrics = validate(
            model=model,
            data_root=Path(config["data_root"]),
            image_size=int(config["image_size"]),
            device=device,
            threshold=float(config["threshold"]),
        )

        log_dict = {"epoch": epoch, "train/loss": train_loss}
        log_dict.update({f"val/{k}": v for k, v in val_metrics.items()})
        wandb.log(log_dict)

        if val_metrics["dice"] > best_dice:
            best_dice = val_metrics["dice"]
            torch.save(
                {
                    "model_state_dict": model.state_dict(),
                    "encoder": str(config["encoder"]),
                    "image_size": int(config["image_size"]),
                    "threshold": float(config["threshold"]),
                },
                best_path,
            )

    artifact = wandb.Artifact(name=f"{wandb_run.name}-best", type="model")
    artifact.add_file(str(best_path))
    wandb.log_artifact(artifact)

    final_metrics = validate(
        model=model,
        data_root=Path(config["data_root"]),
        image_size=int(config["image_size"]),
        device=device,
        threshold=float(config["threshold"]),
    )

    if created_run:
        wandb.finish()
    return final_metrics


def main() -> None:
    args = parse_args()
    config = {
        "data_root": args.data_root,
        "encoder": args.encoder,
        "epochs": args.epochs,
        "image_size": args.image_size,
        "batch_size": args.batch_size,
        "num_workers": args.num_workers,
        "lr": args.lr,
        "weight_decay": args.weight_decay,
        "dice_weight": args.dice_weight,
        "threshold": args.threshold,
        "device": args.device,
        "seed": args.seed,
        "output_dir": args.output_dir,
        "run_name": args.run_name,
        "wandb_project": args.wandb_project,
        "wandb_entity": args.wandb_entity,
    }

    metrics = run_training(config)
    print(json.dumps(metrics, indent=2))

    if args.save_json:
        args.save_json.parent.mkdir(parents=True, exist_ok=True)
        args.save_json.write_text(json.dumps(metrics, indent=2), encoding="utf-8")


if __name__ == "__main__":
    main()
