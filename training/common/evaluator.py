from __future__ import annotations

from collections.abc import Callable
from pathlib import Path

import cv2
import numpy as np
from tqdm import tqdm

from training.common.labels import yolo_polygon_txt_to_mask
from training.common.metrics import binary_metrics, map_metrics_from_ious


PredictorFn = Callable[[Path, tuple[int, int]], np.ndarray]


def evaluate_split(
    images_dir: Path,
    labels_dir: Path | None,
    predictor: PredictorFn,
    ground_truth_source: str = "txt",
    masks_dir: Path | None = None,
    progress_desc: str | None = None,
    verbose: bool = True,
) -> dict[str, float]:
    image_paths = sorted(images_dir.glob("*.png"))
    if not image_paths:
        raise FileNotFoundError(f"No PNG files found in {images_dir}")

    metric_values: dict[str, list[float]] = {
        "dice": [],
        "iou": [],
        "precision": [],
        "recall": [],
        "f1": [],
    }
    iou_values: list[float] = []

    if ground_truth_source not in {"txt", "png"}:
        raise ValueError("ground_truth_source must be one of: txt, png")

    if ground_truth_source == "txt" and labels_dir is None:
        raise ValueError("labels_dir is required when ground_truth_source='txt'")
    if ground_truth_source == "png" and masks_dir is None:
        raise ValueError("masks_dir is required when ground_truth_source='png'")

    iterator = tqdm(
        image_paths,
        desc=progress_desc or f"Evaluating {images_dir.name}",
        unit="img",
        disable=not verbose,
    )
    for image_path in iterator:
        image = cv2.imread(str(image_path), cv2.IMREAD_COLOR)
        if image is None:
            continue
        height, width = image.shape[:2]

        if ground_truth_source == "png":
            mask_path = masks_dir / f"{image_path.stem}.png"
            gt_mask = cv2.imread(str(mask_path), cv2.IMREAD_GRAYSCALE)
            if gt_mask is None:
                gt_mask = np.zeros((height, width), dtype=np.uint8)
            else:
                if gt_mask.shape != (height, width):
                    gt_mask = cv2.resize(gt_mask, (width, height), interpolation=cv2.INTER_NEAREST)
                gt_mask = (gt_mask > 0).astype(np.uint8)
        else:
            label_path = labels_dir / f"{image_path.stem}.txt"
            gt_mask = yolo_polygon_txt_to_mask(label_path=label_path, height=height, width=width)
        pred_mask = predictor(image_path, (height, width))

        if pred_mask.shape != gt_mask.shape:
            pred_mask = cv2.resize(pred_mask.astype(np.uint8), (width, height), interpolation=cv2.INTER_NEAREST)

        metrics = binary_metrics(pred_mask, gt_mask)
        for key in metric_values:
            metric_values[key].append(metrics[key])
        iou_values.append(metrics["iou"])

    result = {key: float(np.mean(values)) if values else 0.0 for key, values in metric_values.items()}
    result.update(map_metrics_from_ious(iou_values))
    return result
