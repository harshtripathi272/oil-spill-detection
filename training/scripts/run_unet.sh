#!/usr/bin/env bash
set -euo pipefail

source ~/miniconda3/etc/profile.d/conda.sh
conda activate torch_cuda124

export WANDB_PROJECT=${WANDB_PROJECT:-oilspill}

python training/unet/train_unet.py \
  --data-root datasets \
  --encoder efficientnet-b0 \
  --epochs 50 \
  --image-size 512 \
  --batch-size 4 \
  --device cuda \
  --run-name unet-efficientnetb0-baseline
