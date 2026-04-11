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

YOLO_SWEEP_ID=${1:-}
UNET_SWEEP_ID=${2:-}

if [[ -z "$YOLO_SWEEP_ID" || -z "$UNET_SWEEP_ID" ]]; then
	echo "Usage: $0 <yolo_sweep_id> <unet_sweep_id>"
	echo "Create IDs first:"
	echo "  wandb sweep training/sweeps/yolo_sweep.yaml"
	echo "  wandb sweep training/sweeps/unet_sweep.yaml"
	exit 1
fi

echo "Starting YOLO sweep agent (50 runs)"
wandb agent --count 50 "$YOLO_SWEEP_ID"

echo "Starting UNet sweep agent (50 runs)"
wandb agent --count 50 "$UNET_SWEEP_ID"
