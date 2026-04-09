from pathlib import Path

import cv2
import numpy as np


def yolo_polygon_txt_to_mask(label_path: Path, height: int, width: int) -> np.ndarray:
    mask = np.zeros((height, width), dtype=np.uint8)
    if not label_path.exists():
        return mask

    content = label_path.read_text(encoding="utf-8").strip()
    if not content:
        return mask

    for line in content.splitlines():
        parts = line.strip().split()
        if len(parts) < 7:
            continue
        coords = [float(v) for v in parts[1:]]
        if len(coords) % 2 != 0:
            continue

        points = []
        for idx in range(0, len(coords), 2):
            x = int(round(coords[idx] * (width - 1)))
            y = int(round(coords[idx + 1] * (height - 1)))
            points.append([x, y])

        if len(points) >= 3:
            poly = np.array(points, dtype=np.int32)
            cv2.fillPoly(mask, [poly], 1)

    return mask
