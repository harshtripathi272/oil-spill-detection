#!/usr/bin/env bash
set -euo pipefail

source ~/miniconda3/etc/profile.d/conda.sh
conda activate torch_cuda124

export WANDB_PROJECT=${WANDB_PROJECT:-oilspill}
UNET_MASK_OVERWRITE=${UNET_MASK_OVERWRITE:-0}

mask_overwrite_args=()
if [[ "$UNET_MASK_OVERWRITE" == "1" ]]; then
  mask_overwrite_args+=(--overwrite)
fi

python -m training.scripts.build_unet_masks \
  --data-root datasets \
  --splits train val \
  "${mask_overwrite_args[@]}"

python training/unet/train_unet.py \
  --data-root datasets \
  --mask-source auto \
  --masks-subdir masks \
  --encoder mit_b3 \
  --epochs 50 \
  --image-size 512 \
  --batch-size 8 \
  --num-workers 0 \
  --lr 0.00006492337024905077 \
  --weight-decay 0.0008877539657121291 \
  --dice-weight 0.7296254631882069 \
  --threshold 0.5938725810266448 \
  --seed 42 \
  --device cuda \
  --run-name unet-mit_b3-best
