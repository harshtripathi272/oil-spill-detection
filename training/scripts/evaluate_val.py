from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import cv2
import numpy as np
import torch
from ultralytics import YOLO

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from training.common.evaluator import evaluate_split
from training.unet.model import build_unet


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Evaluate YOLO or UNet on val split.")
    parser.add_argument("--model-type", choices=["yolo", "unet"], required=True)
    parser.add_argument("--weights", type=Path, required=True)
    parser.add_argument("--data-root", type=Path, default=ROOT / "datasets")
    parser.add_argument("--imgsz", type=int, default=640)
    parser.add_argument("--image-size", type=int, default=512)
    parser.add_argument("--threshold", type=float, default=0.5)
    parser.add_argument("--device", type=str, default="0")
    parser.add_argument("--output", type=Path, default=None)
    return parser.parse_args()


def yolo_predictor(weights: Path, imgsz: int, conf: float, device: str):
    model = YOLO(str(weights))

    def _predict(image_path: Path, orig_shape: tuple[int, int]) -> np.ndarray:
        h, w = orig_shape
        result = model.predict(source=str(image_path), imgsz=imgsz, conf=conf, device=device, verbose=False)[0]
        if result.masks is None or result.masks.data is None:
            return np.zeros((h, w), dtype=np.uint8)
        mask = (result.masks.data.detach().cpu().numpy() > 0.5).any(axis=0).astype(np.uint8)
        if mask.shape != (h, w):
            mask = cv2.resize(mask, (w, h), interpolation=cv2.INTER_NEAREST)
        return mask

    return _predict


def unet_predictor(weights: Path, image_size: int, threshold: float, device: str):
    ckpt = torch.load(weights, map_location="cpu")
    encoder = ckpt.get("encoder", "efficientnet-b0")
    model = build_unet(encoder_name=encoder)
    model.load_state_dict(ckpt["model_state_dict"])
    dev = torch.device(device if torch.cuda.is_available() else "cpu")
    model.to(dev)
    model.eval()

    def _predict(image_path: Path, orig_shape: tuple[int, int]) -> np.ndarray:
        h, w = orig_shape
        image = cv2.imread(str(image_path), cv2.IMREAD_COLOR)
        image = cv2.cvtColor(image, cv2.COLOR_BGR2RGB)
        resized = cv2.resize(image, (image_size, image_size), interpolation=cv2.INTER_AREA)
        tensor = torch.from_numpy(resized).permute(2, 0, 1).float().unsqueeze(0) / 255.0
        tensor = tensor.to(dev)

        with torch.no_grad():
            probs = torch.sigmoid(model(tensor))[0, 0].detach().cpu().numpy()
        mask = (probs >= threshold).astype(np.uint8)
        if mask.shape != (h, w):
            mask = cv2.resize(mask, (w, h), interpolation=cv2.INTER_NEAREST)
        return mask

    return _predict


def main() -> None:
    args = parse_args()

    if args.model_type == "yolo":
        predictor = yolo_predictor(args.weights, args.imgsz, args.threshold, args.device)
    else:
        predictor = unet_predictor(args.weights, args.image_size, args.threshold, args.device)

    metrics = evaluate_split(
        images_dir=args.data_root / "images/val",
        labels_dir=args.data_root / "labels/val",
        predictor=predictor,
        verbose=True,
    )

    print(json.dumps(metrics, indent=2))
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(metrics, indent=2), encoding="utf-8")


if __name__ == "__main__":
    main()
