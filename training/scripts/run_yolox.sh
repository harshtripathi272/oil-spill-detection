#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
ROOT=$(cd "$SCRIPT_DIR/../.." && pwd)

source ~/miniconda3/etc/profile.d/conda.sh
conda activate torch-gpu

export WANDB_PROJECT=${WANDB_PROJECT:-oilspill_yolo}

if [ ! -d "$ROOT/datasets_sliced/images/train" ]; then
    echo "Generating sliced 640x640 dataset..."
    python3 "$ROOT/training/scripts/slice_dataset.py" --src "$ROOT/datasets" --dst "$ROOT/datasets_sliced" --slice-size 640 --overlap 0.2
else
    echo "Sliced dataset already exists, skipping generation."
fi

echo "Starting YOLO-Large Training with Built-in Augmentations..."

# Note: albumentations is handled automatically by Ultralytics if installed. 
# It is not a direct CLI argument.
yolo detect train \
  data="$ROOT/datasets_sliced/data.yaml" \
  model=$ROOT/../yolo26x.pt\
  epochs=150 \
  patience=20 \
  imgsz=640 \
  batch=16 \
  workers=8 \
  device=0 \
  project="$ROOT/runs/yolox" \
  name="yolo26x-sliced-640" \
  hsv_h=0.015 \
  hsv_s=0.7 \
  hsv_v=0.4 \
  degrees=10.0 \
  translate=0.1 \
  scale=0.5 \
  shear=0.0 \
  perspective=0.0 \
  flipud=0.5 \
  fliplr=0.5 \
  mosaic=1.0 \
  mixup=0.1
