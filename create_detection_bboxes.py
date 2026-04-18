from __future__ import annotations

import argparse
import shutil
from dataclasses import dataclass
from pathlib import Path

import cv2
import numpy as np
from tqdm import tqdm


IMAGE_EXTENSIONS = {".png", ".jpg", ".jpeg", ".tif", ".tiff", ".bmp"}


@dataclass(frozen=True)
class BBox:
    x_min: int
    y_min: int
    x_max: int
    y_max: int

    @property
    def width(self) -> int:
        return self.x_max - self.x_min

    @property
    def height(self) -> int:
        return self.y_max - self.y_min


def parse_args() -> argparse.Namespace:
    repo_dir = Path(__file__).resolve().parent

    parser = argparse.ArgumentParser(
        description=(
            "Create YOLO object-detection labels from segmentation masks. "
            "Each connected oil patch becomes a separate bounding box."
        )
    )
    parser.add_argument(
        "--dataset-root",
        type=Path,
        default=repo_dir / "datasets",
        help="Dataset root that contains masks/train and masks/val.",
    )
    parser.add_argument(
        "--masks-dir-name",
        type=str,
        default="masks",
        help="Name of masks directory under dataset root.",
    )
    parser.add_argument(
        "--output-labels-dir-name",
        type=str,
        default="labels_bbox",
        help="Name of output labels directory under dataset root.",
    )
    parser.add_argument(
        "--class-id",
        type=int,
        default=0,
        help="Class ID to write in YOLO labels.",
    )
    parser.add_argument(
        "--threshold",
        type=int,
        default=0,
        help="Mask threshold. Pixels > threshold are considered oil unless --mask-value is used.",
    )
    parser.add_argument(
        "--mask-value",
        type=int,
        default=None,
        help="If set, only pixels exactly equal to this value are considered oil.",
    )
    parser.add_argument(
        "--min-area",
        type=int,
        default=10,
        help="Minimum connected-component area (in pixels) to keep as a bbox.",
    )
    parser.add_argument(
        "--min-width",
        type=int,
        default=20,
        help=(
            "Minimum bbox width in source-image pixels. "
            "For 2048px source and 1024 training size, 20px ~= 10px effective size."
        ),
    )
    parser.add_argument(
        "--min-height",
        type=int,
        default=20,
        help=(
            "Minimum bbox height in source-image pixels. "
            "For 2048px source and 1024 training size, 20px ~= 10px effective size."
        ),
    )
    parser.add_argument(
        "--disable-merge",
        action="store_true",
        help="Disable post-processing that merges nearby fragmented boxes.",
    )
    parser.add_argument(
        "--merge-gap",
        type=int,
        default=12,
        help="Merge boxes whose edges are within this many pixels.",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Delete output labels directory before generating files.",
    )
    return parser.parse_args()


def load_mask_as_binary(mask_path: Path, threshold: int, mask_value: int | None) -> np.ndarray:
    # Read unchanged to preserve single-channel masks and any integer values.
    mask = cv2.imread(str(mask_path), cv2.IMREAD_UNCHANGED)
    if mask is None:
        raise RuntimeError(f"Failed to read mask: {mask_path}")

    if mask.ndim == 3:
        # If mask is RGB(A), any non-zero channel marks foreground by default.
        if mask_value is None:
            binary = np.any(mask > threshold, axis=2)
        else:
            binary = np.any(mask == mask_value, axis=2)
    else:
        if mask_value is None:
            binary = mask > threshold
        else:
            binary = mask == mask_value

    return binary.astype(np.uint8)


def find_patch_bboxes(binary_mask: np.ndarray, min_area: int, min_width: int, min_height: int) -> list[BBox]:
    if not np.any(binary_mask):
        return []

    num_labels, _, stats, _ = cv2.connectedComponentsWithStats(binary_mask, connectivity=8)
    bboxes: list[BBox] = []

    # Label 0 is background.
    for label_idx in range(1, num_labels):
        x = int(stats[label_idx, cv2.CC_STAT_LEFT])
        y = int(stats[label_idx, cv2.CC_STAT_TOP])
        w = int(stats[label_idx, cv2.CC_STAT_WIDTH])
        h = int(stats[label_idx, cv2.CC_STAT_HEIGHT])
        area = int(stats[label_idx, cv2.CC_STAT_AREA])

        if area < min_area or w < min_width or h < min_height:
            continue

        bboxes.append(BBox(x_min=x, y_min=y, x_max=x + w, y_max=y + h))

    return bboxes


def _boxes_overlap_or_close(a: BBox, b: BBox, gap: int) -> bool:
    return not (
        (a.x_max + gap < b.x_min)
        or (b.x_max + gap < a.x_min)
        or (a.y_max + gap < b.y_min)
        or (b.y_max + gap < a.y_min)
    )


def _merge_pair(a: BBox, b: BBox) -> BBox:
    return BBox(
        x_min=min(a.x_min, b.x_min),
        y_min=min(a.y_min, b.y_min),
        x_max=max(a.x_max, b.x_max),
        y_max=max(a.y_max, b.y_max),
    )


def merge_fragmented_bboxes(bboxes: list[BBox], merge_gap: int) -> list[BBox]:
    if len(bboxes) < 2:
        return bboxes

    merged = list(bboxes)
    changed = True
    while changed:
        changed = False
        out: list[BBox] = []
        used = [False] * len(merged)

        for i, current in enumerate(merged):
            if used[i]:
                continue

            for j in range(i + 1, len(merged)):
                if used[j]:
                    continue
                if _boxes_overlap_or_close(current, merged[j], gap=merge_gap):
                    current = _merge_pair(current, merged[j])
                    used[j] = True
                    changed = True

            used[i] = True
            out.append(current)

        merged = out

    return merged


def bbox_to_yolo_line(bbox: BBox, image_width: int, image_height: int, class_id: int) -> str:
    x_center = ((bbox.x_min + bbox.x_max) / 2.0) / float(image_width)
    y_center = ((bbox.y_min + bbox.y_max) / 2.0) / float(image_height)
    width = bbox.width / float(image_width)
    height = bbox.height / float(image_height)
    return f"{class_id} {x_center:.6f} {y_center:.6f} {width:.6f} {height:.6f}"


def write_label_file(label_path: Path, lines: list[str]) -> None:
    label_path.parent.mkdir(parents=True, exist_ok=True)
    with label_path.open("w", encoding="utf-8") as handle:
        for line in lines:
            handle.write(f"{line}\n")


def iter_mask_files(mask_split_dir: Path) -> list[Path]:
    return sorted(
        path for path in mask_split_dir.iterdir() if path.is_file() and path.suffix.lower() in IMAGE_EXTENSIONS
    )


def prepare_output_dir(output_dir: Path, overwrite: bool) -> None:
    if output_dir.exists() and overwrite:
        shutil.rmtree(output_dir)
    (output_dir / "train").mkdir(parents=True, exist_ok=True)
    (output_dir / "val").mkdir(parents=True, exist_ok=True)


def process_split(
    split: str,
    masks_split_dir: Path,
    output_split_dir: Path,
    class_id: int,
    threshold: int,
    mask_value: int | None,
    min_area: int,
    min_width: int,
    min_height: int,
    disable_merge: bool,
    merge_gap: int,
) -> tuple[int, int]:
    mask_files = iter_mask_files(masks_split_dir)
    images_seen = 0
    bboxes_written = 0

    progress = tqdm(mask_files, desc=f"Generating {split} bboxes", unit="mask")
    for mask_path in progress:
        binary = load_mask_as_binary(mask_path, threshold=threshold, mask_value=mask_value)
        image_height, image_width = binary.shape[:2]
        bboxes = find_patch_bboxes(
            binary,
            min_area=min_area,
            min_width=min_width,
            min_height=min_height,
        )
        if not disable_merge and bboxes:
            bboxes = merge_fragmented_bboxes(bboxes, merge_gap=max(0, merge_gap))

        label_lines = [bbox_to_yolo_line(bbox, image_width, image_height, class_id) for bbox in bboxes]
        label_path = output_split_dir / f"{mask_path.stem}.txt"
        write_label_file(label_path, label_lines)

        images_seen += 1
        bboxes_written += len(bboxes)

    return images_seen, bboxes_written


def main() -> None:
    args = parse_args()

    masks_root = args.dataset_root / args.masks_dir_name
    output_labels_root = args.dataset_root / args.output_labels_dir_name

    for split in ("train", "val"):
        split_dir = masks_root / split
        if not split_dir.exists():
            raise FileNotFoundError(f"Missing split directory: {split_dir}")

    prepare_output_dir(output_labels_root, overwrite=args.overwrite)

    total_images = 0
    total_bboxes = 0

    for split in ("train", "val"):
        images_seen, bboxes_written = process_split(
            split=split,
            masks_split_dir=masks_root / split,
            output_split_dir=output_labels_root / split,
            class_id=args.class_id,
            threshold=args.threshold,
            mask_value=args.mask_value,
            min_area=args.min_area,
            min_width=args.min_width,
            min_height=args.min_height,
            disable_merge=args.disable_merge,
            merge_gap=args.merge_gap,
        )
        total_images += images_seen
        total_bboxes += bboxes_written
        print(f"{split}: images={images_seen}, bboxes={bboxes_written}")

    print(f"Output labels directory: {output_labels_root.resolve()}")
    print(f"Total images processed: {total_images}")
    print(f"Total bboxes written: {total_bboxes}")


if __name__ == "__main__":
    main()
