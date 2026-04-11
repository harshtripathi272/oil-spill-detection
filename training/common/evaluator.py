from __future__ import annotations

from collections.abc import Callable
from pathlib import Path

import cv2
import numpy as np
from tqdm import tqdm

from training.common.labels import yolo_polygon_txt_to_mask
from training.common.metrics import binary_metrics


PredictorFn = Callable[[Path, tuple[int, int]], np.ndarray]


def evaluate_split(
    images_dir: Path,
    labels_dir: Path,
    predictor: PredictorFn,
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

    iterator = tqdm(image_paths, desc=f"Evaluating {images_dir.name}", unit="img", disable=not verbose)
    for image_path in iterator:
        image = cv2.imread(str(image_path), cv2.IMREAD_COLOR)
        if image is None:
            continue
        height, width = image.shape[:2]

        label_path = labels_dir / f"{image_path.stem}.txt"
        gt_mask = yolo_polygon_txt_to_mask(label_path=label_path, height=height, width=width)
        pred_mask = predictor(image_path, (height, width))

        if pred_mask.shape != gt_mask.shape:
            pred_mask = cv2.resize(pred_mask.astype(np.uint8), (width, height), interpolation=cv2.INTER_NEAREST)

        metrics = binary_metrics(pred_mask, gt_mask)
        for key in metric_values:
            metric_values[key].append(metrics[key])

    return {key: float(np.mean(values)) if values else 0.0 for key, values in metric_values.items()}
