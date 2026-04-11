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
  --encoder efficientnet-b0 \
  --epochs 50 \
  --image-size 512 \
  --batch-size 4 \
  --device cuda \
  --run-name unet-efficientnetb0-baseline
