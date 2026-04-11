from __future__ import annotations

import numpy as np


def binary_metrics(pred: np.ndarray, target: np.ndarray, eps: float = 1e-8) -> dict[str, float]:
    pred_bool = pred.astype(bool)
    target_bool = target.astype(bool)

    tp = np.logical_and(pred_bool, target_bool).sum(dtype=np.float64)
    fp = np.logical_and(pred_bool, np.logical_not(target_bool)).sum(dtype=np.float64)
    fn = np.logical_and(np.logical_not(pred_bool), target_bool).sum(dtype=np.float64)
    tn = np.logical_and(np.logical_not(pred_bool), np.logical_not(target_bool)).sum(dtype=np.float64)

    precision = tp / (tp + fp + eps)
    recall = tp / (tp + fn + eps)
    f1 = (2.0 * precision * recall) / (precision + recall + eps)
    iou = tp / (tp + fp + fn + eps)
    dice = (2.0 * tp) / (2.0 * tp + fp + fn + eps)

    return {
        "dice": float(dice),
        "iou": float(iou),
        "precision": float(precision),
        "recall": float(recall),
        "f1": float(f1),
        "tp": float(tp),
        "fp": float(fp),
        "fn": float(fn),
        "tn": float(tn),
    }


def map_metrics_from_ious(ious: list[float]) -> dict[str, float]:
    if not ious:
        return {"map50": 0.0, "map50_95": 0.0}

    iou_arr = np.asarray(ious, dtype=np.float32)
    thresholds = np.arange(0.50, 1.00, 0.05, dtype=np.float32)
    ap_by_threshold = [float(np.mean(iou_arr >= thr)) for thr in thresholds]

    return {
        "map50": ap_by_threshold[0],
        "map50_95": float(np.mean(ap_by_threshold)),
    }
