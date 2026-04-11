from __future__ import annotations

from pathlib import Path

import cv2
import numpy as np
import torch
from torch.utils.data import Dataset

from training.common.labels import yolo_polygon_txt_to_mask


class OilSpillSegmentationDataset(Dataset):
    """Memory-safe dataset that reads one image and label from disk per sample."""

    def __init__(
        self,
        root_dir: Path,
        split: str,
        image_size: int = 512,
        mask_source: str = "auto",
        masks_subdir: str = "masks",
    ):
        self.root_dir = Path(root_dir)
        self.split = split
        self.image_size = image_size
        self.mask_source = mask_source
        self.images_dir = self.root_dir / f"images/{split}"
        self.labels_dir = self.root_dir / f"labels/{split}"
        self.masks_dir = self.root_dir / f"{masks_subdir}/{split}"

        self.image_paths = sorted(self.images_dir.glob("*.png"))
        if not self.image_paths:
            raise FileNotFoundError(f"No images found in {self.images_dir}")

        if self.mask_source not in {"auto", "png", "txt"}:
            raise ValueError("mask_source must be one of: auto, png, txt")

    def __len__(self) -> int:
        return len(self.image_paths)

    def __getitem__(self, idx: int) -> tuple[torch.Tensor, torch.Tensor]:
        image_path = self.image_paths[idx]
        label_path = self.labels_dir / f"{image_path.stem}.txt"
        mask_png_path = self.masks_dir / f"{image_path.stem}.png"

        image = cv2.imread(str(image_path), cv2.IMREAD_COLOR)
        if image is None:
            raise RuntimeError(f"Failed to read image: {image_path}")
        image = cv2.cvtColor(image, cv2.COLOR_BGR2RGB)

        height, width = image.shape[:2]
        use_png_mask = self.mask_source == "png" or (
            self.mask_source == "auto" and mask_png_path.exists()
        )

        if use_png_mask:
            mask = cv2.imread(str(mask_png_path), cv2.IMREAD_GRAYSCALE)
            if mask is None:
                raise RuntimeError(f"Failed to read mask: {mask_png_path}")
            mask = (mask > 0).astype(np.uint8)
        else:
            mask = yolo_polygon_txt_to_mask(label_path=label_path, height=height, width=width)

        if self.image_size is not None:
            image = cv2.resize(image, (self.image_size, self.image_size), interpolation=cv2.INTER_AREA)
            mask = cv2.resize(mask, (self.image_size, self.image_size), interpolation=cv2.INTER_NEAREST)

        image_tensor = torch.from_numpy(image).permute(2, 0, 1).float() / 255.0
        mask_tensor = torch.from_numpy(mask).unsqueeze(0).float()

        return image_tensor, mask_tensor
