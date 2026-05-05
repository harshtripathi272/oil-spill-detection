# Test SAR Inference Script

## Usage Examples

### Test with YOLO bbox model (default):
python scripts/test_sar_inference.py \\
  --image sentinel_data/downloads/S1A_IW_GRDH_1SDV_20240101T000000_20240101T000026_051234_063456.h5 \\
  --model runs/yolo/yolo26n-bbox-1024-merged/weights/best.pt \\
  --task detect

### Test with YOLO segmentation model:
python scripts/test_sar_inference.py \\
  --image sentinel_data/downloads/some_sar_file.tiff \\
  --model /models/yolo11s-seg.pt \\
  --task segment

### Skip preprocessing (if image is already preprocessed):
python scripts/test_sar_inference.py \
  --image sentinel_data/preprocessed/my_image.png \
  --model /models/yolo11s-seg.pt \
  --skip-preprocessing

## What it does:
1. Converts raw SAR (.h5, .tiff) to PNG format
2. Applies full SAR preprocessing (Lee filter, CLAHE, denoising, etc.)
3. Runs inference with specified model
4. Outputs JSON results to console

## Output format:
{
  "prediction": "oil_spill" | "no_oil_spill",
  "confidence": 0.85,
  "model_type": "yolo_detect",
  "detections": 2
}
