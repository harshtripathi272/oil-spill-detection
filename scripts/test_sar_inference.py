#!/usr/bin/env python3
"""
Test script for running inference on Sentinel SAR images.

Usage:
    python test_sar_inference.py --image <path_to_sar_file> --model <path_to_model> [--model-type yolo]

This script:
1. Converts raw SAR files (.h5, .tiff) to PNG format
2. Applies preprocessing (lee filter, CLAHE, denoising, etc.)
3. Runs inference using the specified model
4. Outputs JSON results
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

from preprocessing.apply_sar_processing import preprocess_sar_png
from training.unet.model import build_unet


def convert_sar_to_png(sar_path: str, output_dir: str = "/data/user13/oilspill_ugq/oil-spill-detection/sentinel_data/preprocessed") -> str:
    """Convert raw SAR file to PNG format."""
    path = Path(sar_path)
    suffix = path.suffix.lower()

    # Check if this is a BURST image (not suitable for current preprocessing)
    filename = path.name.upper()
    if "BURST" in filename:
        raise RuntimeError(
            f"BURST images are not supported for inference. "
            f"Current preprocessing pipeline expects RTC (Radiometrically Terrain Corrected) images. "
            f"File: {path.name}"
        )

    if suffix in {".png", ".jpg", ".jpeg"}:
        return str(path)

    Path(output_dir).mkdir(parents=True, exist_ok=True)

    if suffix in {".tif", ".tiff"}:
        try:
            import tifffile as tiff
            arr = tiff.imread(str(path))
        except ImportError:
            arr = cv2.imread(str(path), cv2.IMREAD_UNCHANGED)

    elif suffix == ".h5":
        try:
            import h5py
            with h5py.File(str(path), 'r') as h5_file:
                arr = _find_h5_sar_dataset(h5_file)
                if arr is None:
                    raise RuntimeError(
                        f"No suitable SAR dataset found in {path}. "
                        "This HDF5 file does not contain a direct 2D/3D SAR image dataset "
                        "that the test converter can use."
                    )
        except ImportError:
            raise RuntimeError("h5py required for .h5 files")

    else:
        raise RuntimeError(f"Unsupported file type: {suffix}")


def _find_h5_sar_dataset(group):
    try:
        import h5py
    except ImportError:
        return None

    if isinstance(group, h5py.Dataset) and group.ndim in {2, 3}:
        return group[()]

    if isinstance(group, h5py.Group):
        for _, item in group.items():
            arr = _find_h5_sar_dataset(item)
            if arr is not None:
                return arr

    return None

    # Normalize to uint8
    if arr.ndim == 2:
        arr = np.stack([arr] * 3, axis=-1)
    elif arr.ndim == 3:
        if arr.shape[0] in {1, 2, 3} and arr.shape[-1] not in {1, 2, 3}:
            arr = np.moveaxis(arr, 0, -1)
        if arr.shape[-1] == 1:
            arr = np.concatenate([arr] * 3, axis=-1)
        elif arr.shape[-1] == 2:
            arr = np.concatenate([arr, arr[..., :1]], axis=-1)
        elif arr.shape[-1] > 3:
            arr = arr[..., :3]

    arr = arr.astype(np.float32)
    min_val = np.nanmin(arr)
    max_val = np.nanmax(arr)
    if max_val > min_val:
        normalized = (arr - min_val) / (max_val - min_val)
        normalized = np.clip(normalized * 255.0, 0, 255)
        arr_uint8 = normalized.astype(np.uint8)
    else:
        arr_uint8 = np.zeros(arr.shape, dtype=np.uint8)

    output_path = Path(output_dir) / f"{path.stem}_converted.png"
    cv2.imwrite(str(output_path), cv2.cvtColor(arr_uint8, cv2.COLOR_RGB2BGR))
    return str(output_path)


def run_inference(image_path: str, model_path: str, model_type: str = "yolo",
                  conf: float = 0.5, threshold: float = 0.5,
                  imgsz: int = 640, image_size: int = 512, task: str = "detect"):
    """Run inference on preprocessed image."""

    if model_type == "yolo":
        model = YOLO(model_path)
        result = model.predict(
            source=image_path,
            imgsz=imgsz,
            conf=conf,
            device="0",
            verbose=False
        )[0]

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

    else:  # unet
        ckpt = torch.load(model_path, map_location="cpu")
        encoder = ckpt.get("encoder", "efficientnet-b0")

        model = build_unet(encoder_name=encoder)
        model.load_state_dict(ckpt["model_state_dict"])
        dev = torch.device("cuda" if torch.cuda.is_available() else "cpu")
        model.to(dev)
        model.eval()

        image = cv2.imread(image_path, cv2.IMREAD_COLOR)
        if image is None:
            raise FileNotFoundError(f"Cannot read image: {image_path}")

        h, w = image.shape[:2]
        image_rgb = cv2.cvtColor(image, cv2.COLOR_BGR2RGB)
        resized = cv2.resize(image_rgb, (image_size, image_size), interpolation=cv2.INTER_AREA)
        tensor = torch.from_numpy(resized).permute(2, 0, 1).float().unsqueeze(0) / 255.0
        tensor = tensor.to(dev)

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
    parser = argparse.ArgumentParser(description="Test SAR inference pipeline")
    parser.add_argument("--image", type=str, default="/data/user13/oilspill_ugq/oil-spill-detection/sentinel_data/downloads/OPERA_L2_RTC-S1_T019-038870-IW1_20260430T234724Z_20260501T083346Z_S1A_30_v1.0.h5",help="Path to Sentinel SAR file (.h5, .tiff, .png)")
    parser.add_argument("--model", type=str, required=True, help="Path to model weights")
    parser.add_argument("--model-type", choices=["yolo", "unet"], default="yolo", help="Model type")
    parser.add_argument("--task", choices=["detect", "segment"], default="detect", help="YOLO task type (detect=bbox, segment=mask)")
    parser.add_argument("--output-dir", type=str, default="/data/user13/oilspill_ugq/oil-spill-detection/sentinel_data/preprocessed", help="Output directory for conversions")
    parser.add_argument("--conf", type=float, default=0.5, help="Confidence threshold (YOLO)")
    parser.add_argument("--threshold", type=float, default=0.5, help="Pixel threshold (UNet)")
    parser.add_argument("--imgsz", type=int, default=640, help="Image size (YOLO)")
    parser.add_argument("--image-size", type=int, default=512, help="Image size (UNet)")
    parser.add_argument("--skip-preprocessing", action="store_true", help="Skip SAR preprocessing (assume image is already preprocessed)")

    args = parser.parse_args()

    try:
        print(f"🔍 Testing inference on: {args.image}")
        print(f"🤖 Using model: {args.model} ({args.model_type}, task: {args.task})")

        # Step 1: Convert SAR to PNG if needed
        if args.skip_preprocessing:
            converted_path = args.image
            print("⏭️ Skipping conversion (assuming already PNG)")
        else:
            print("🔄 Converting SAR to PNG...")
            converted_path = convert_sar_to_png(args.image, args.output_dir)
            print(f"✅ Converted to: {converted_path}")

        # Step 2: Apply preprocessing
        if args.skip_preprocessing:
            preprocessed_path = converted_path
            print("⏭️  Skipping preprocessing")
        else:
            print("🔄 Applying SAR preprocessing...")
            preprocessed_path = Path(args.output_dir) / f"{Path(converted_path).stem}_preprocessed.png"
            preprocess_sar_png(
                input_path=Path(converted_path),
                output_path=preprocessed_path,
            )
            print(f"✅ Preprocessed to: {preprocessed_path}")

        # Step 3: Run inference
        print("🧠 Running inference...")
        result = run_inference(
            image_path=str(preprocessed_path),
            model_path=args.model,
            model_type=args.model_type,
            conf=args.conf,
            threshold=args.threshold,
            imgsz=args.imgsz,
            image_size=args.image_size,
            task=args.task,
        )

        # Step 4: Output results
        print("📊 Results:")
        print(json.dumps(result, indent=2))

        return 0

    except Exception as e:
        print(f"❌ Error: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())
