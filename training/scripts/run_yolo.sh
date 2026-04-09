#!/usr/bin/env bash
set -euo pipefail

source ~/miniconda3/etc/profile.d/conda.sh
conda activate torch_cuda124

export WANDB_PROJECT=${WANDB_PROJECT:-oilspill}

python training/yolo/train_yolo.py \
  --data datasets/dataset.yaml \
  --model yolo11x-seg.pt \
  --epochs 50 \
  --imgsz 640 \
  --batch 16 \
  --device 1 \
  --run-name yolo11x-seg-baseline
