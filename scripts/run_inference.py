#!/usr/bin/env python3
"""
Simple inference script for oil spill detection.

Usage:
    python run_inference.py --image <path_to_image> --model <path_to_model> [--model-type yolo]

Outputs JSON to stdout with keys:
    - prediction: "oil_spill" or "no_oil_spill"
    - confidence: float confidence score
"""

import argparse
import json
import sys
from pathlib import Path

import cv2
import numpy as np
import torch
from ultralytics import YOLO

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from training.unet.model import build_unet


def yolo_inference(image_path: str, model_path: str, conf: float = 0.5, imgsz: int = 640, task: str = "detect"):
    """Run YOLO inference on single image."""
    filename = Path(image_path).name.upper()
    if "BURST" in filename:
        raise RuntimeError(
            f"BURST images are not supported for inference. "
            f"Current preprocessing pipeline expects RTC-like inputs. File: {filename}"
        )

    model = YOLO(model_path)
    result = model.predict(
        source=image_path,
        imgsz=imgsz,
        conf=conf,
        device="0",
        verbose=False
    )[0]

    # Check if oil spill detected based on task type
    if task == "segment":
        has_detection = result.masks is not None and result.masks.data is not None
    else:  # detect (bbox)
        has_detection = result.boxes is not None and len(result.boxes) > 0

    # Get confidence from boxes (works for both detect and segment tasks)
    confidence = 0.0
    if result.boxes is not None and hasattr(result.boxes, 'conf') and len(result.boxes.conf) > 0:
        confidence = float(result.boxes.conf[0])

    return {
        "prediction": "oil_spill" if has_detection and confidence > conf else "no_oil_spill",
        "confidence": confidence,
        "model_type": f"yolo_{task}",
        "detections": len(result.boxes) if result.boxes else 0,
    }


def unet_inference(image_path: str, model_path: str, threshold: float = 0.5, image_size: int = 512):
    """Run UNet inference on single image."""
    # Load checkpoint
    ckpt = torch.load(model_path, map_location="cpu")
    encoder = ckpt.get("encoder", "efficientnet-b0")
    
    # Build and load model
    model = build_unet(encoder_name=encoder)
    model.load_state_dict(ckpt["model_state_dict"])
    dev = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    model.to(dev)
    model.eval()

    # Load and preprocess image
    image = cv2.imread(image_path, cv2.IMREAD_COLOR)
    if image is None:
        raise FileNotFoundError(f"Cannot read image: {image_path}")

    h, w = image.shape[:2]
    image_rgb = cv2.cvtColor(image, cv2.COLOR_BGR2RGB)
    resized = cv2.resize(image_rgb, (image_size, image_size), interpolation=cv2.INTER_AREA)
    tensor = torch.from_numpy(resized).permute(2, 0, 1).float().unsqueeze(0) / 255.0
    tensor = tensor.to(dev)

    # Run inference
    with torch.no_grad():
        probs = torch.sigmoid(model(tensor))[0, 0].detach().cpu().numpy()

    mask = (probs >= threshold).astype(np.uint8)
    pixel_ratio = np.sum(mask) / (h * w)
    has_detection = pixel_ratio > 0.01  # >1% of pixels flagged
    confidence = float(np.mean(probs))

    return {
        "prediction": "oil_spill" if has_detection else "no_oil_spill",
        "confidence": confidence,
        "pixel_ratio": pixel_ratio,
        "model_type": "unet",
    }


def main():
    parser = argparse.ArgumentParser(description="Run oil spill detection inference on single image")
    parser.add_argument("--image", type=str, required=True, help="Path to input image")
    parser.add_argument("--model", type=str, required=True, help="Path to model weights")
    parser.add_argument("--model-type", choices=["yolo", "unet"], default="yolo", help="Model type")
    parser.add_argument("--task", choices=["detect", "segment"], default="detect", help="YOLO task type (detect=bbox, segment=mask)")
    parser.add_argument("--conf", type=float, default=0.5, help="Confidence threshold (YOLO)")
    parser.add_argument("--threshold", type=float, default=0.5, help="Pixel threshold (UNet)")
    parser.add_argument("--imgsz", type=int, default=640, help="Image size (YOLO)")
    parser.add_argument("--image-size", type=int, default=512, help="Image size (UNet)")

    args = parser.parse_args()

    try:
        if args.model_type == "yolo":
            result = yolo_inference(args.image, args.model, args.conf, args.imgsz, args.task)
        else:
            result = unet_inference(args.image, args.model, args.threshold, args.image_size)

        # Output JSON to stdout
        print(json.dumps(result))
        return 0

    except Exception as e:
        error_result = {
            "prediction": "error",
            "confidence": 0.0,
            "error": str(e),
        }
        print(json.dumps(error_result), file=sys.stderr)
        print(json.dumps(error_result))
        return 1


if __name__ == "__main__":
    sys.exit(main())
