#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
ROOT=$(cd "$SCRIPT_DIR/../.." && pwd)

source ~/miniconda3/etc/profile.d/conda.sh
conda activate torch-gpu

export WANDB_PROJECT=${WANDB_PROJECT:-oilspill}

# Defaults are set for merged bbox training at 1024 with YOLO26x.
MODEL_PATH=${MODEL_PATH:-$ROOT/../yolo26x.pt}
IMAGES_ROOT=${IMAGES_ROOT:-$ROOT/datasets/images}
LABELS_BBOX_ROOT=${LABELS_BBOX_ROOT:-$ROOT/datasets/labels_bbox}
PREPARED_DATASET_ROOT=${PREPARED_DATASET_ROOT:-$ROOT/datasets/bbox_augmented}
PROJECT_DIR=${PROJECT_DIR:-$ROOT/runs/yolo}
RUN_NAME=${RUN_NAME:-yolo26x-bbox-1024-merged}

EPOCHS=${EPOCHS:-100}
IMGSZ=${IMGSZ:-1024}
BATCH=${BATCH:-16}
WORKERS=${WORKERS:-8}
DEVICE=${DEVICE:-0}
AUG_PER_IMAGE=${AUG_PER_IMAGE:-2}
WANDB_MODE=${WANDB_MODE:-online}

if [ ! -f "$MODEL_PATH" ]; then
    echo "ERROR: Model file not found: $MODEL_PATH"
    echo "Set MODEL_PATH to a valid checkpoint, then rerun."
    exit 1
fi

echo "Validating model checkpoint: $MODEL_PATH"
if ! python3 - <<PY
from ultralytics import YOLO
YOLO(r"$MODEL_PATH")
print("checkpoint_ok")
PY
then
    echo "ERROR: Invalid or corrupted model checkpoint: $MODEL_PATH"
    echo "The current yolo26x.pt cannot be loaded by Ultralytics."
    echo "Replace the file and rerun, or override with a valid model, e.g.:"
    echo "  MODEL_PATH=$ROOT/../yolo26l.pt bash $ROOT/training/scripts/run_yolo.sh"
    exit 1
fi

echo "Starting merged bbox YOLO training"
echo "Model: $MODEL_PATH"
echo "Run name: $RUN_NAME"
echo "Image size: $IMGSZ"

python3 "$ROOT/training/train_yolo26_bbox.py" \
    --prefer-preprocessed \
    --images-root "$IMAGES_ROOT" \
    --labels-bbox-root "$LABELS_BBOX_ROOT" \
    --prepared-dataset-root "$PREPARED_DATASET_ROOT" \
    --model "$MODEL_PATH" \
    --epochs "$EPOCHS" \
    --imgsz "$IMGSZ" \
    --batch "$BATCH" \
    --workers "$WORKERS" \
    --device "$DEVICE" \
    --project "$PROJECT_DIR" \
    --name "$RUN_NAME" \
    --aug-per-image "$AUG_PER_IMAGE" \
    --wandb-project "$WANDB_PROJECT" \
    --wandb-mode "$WANDB_MODE"
