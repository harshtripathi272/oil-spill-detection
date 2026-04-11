from __future__ import annotations

import argparse
import sys
from pathlib import Path

import cv2
from tqdm import tqdm

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from training.common.labels import yolo_polygon_txt_to_mask


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build PNG binary masks for UNet from YOLO polygon label txt files."
    )
    parser.add_argument("--data-root", type=Path, default=ROOT / "datasets")
    parser.add_argument("--images-subdir", type=str, default="images")
    parser.add_argument("--labels-subdir", type=str, default="labels")
    parser.add_argument("--masks-subdir", type=str, default="masks")
    parser.add_argument("--splits", nargs="+", default=["train", "val"])
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Rewrite existing mask files if they already exist.",
    )
    return parser.parse_args()


def build_split_masks(
    data_root: Path,
    split: str,
    images_subdir: str,
    labels_subdir: str,
    masks_subdir: str,
    overwrite: bool,
) -> tuple[int, int]:
    images_dir = data_root / images_subdir / split
    labels_dir = data_root / labels_subdir / split
    masks_dir = data_root / masks_subdir / split
    masks_dir.mkdir(parents=True, exist_ok=True)

    image_paths = sorted(images_dir.glob("*.png"))
    if not image_paths:
        raise FileNotFoundError(f"No PNG images found in {images_dir}")

    written = 0
    skipped = 0
    pbar = tqdm(image_paths, desc=f"Building {split} masks", unit="img")
    for image_path in pbar:
        out_path = masks_dir / f"{image_path.stem}.png"
        if out_path.exists() and not overwrite:
            skipped += 1
            continue

        image = cv2.imread(str(image_path), cv2.IMREAD_COLOR)
        if image is None:
            raise RuntimeError(f"Failed to read image: {image_path}")
        height, width = image.shape[:2]

        label_path = labels_dir / f"{image_path.stem}.txt"
        mask = yolo_polygon_txt_to_mask(label_path=label_path, height=height, width=width)
        if not cv2.imwrite(str(out_path), (mask * 255).astype("uint8")):
            raise RuntimeError(f"Failed to write mask: {out_path}")
        written += 1

    return written, skipped


def main() -> None:
    args = parse_args()

    total_written = 0
    total_skipped = 0
    for split in args.splits:
        written, skipped = build_split_masks(
            data_root=args.data_root,
            split=split,
            images_subdir=args.images_subdir,
            labels_subdir=args.labels_subdir,
            masks_subdir=args.masks_subdir,
            overwrite=args.overwrite,
        )
        total_written += written
        total_skipped += skipped
        print(f"[{split}] written={written}, skipped={skipped}")

    print(f"Total masks written: {total_written}")
    print(f"Total masks skipped: {total_skipped}")
    print(f"Output root: {(args.data_root / args.masks_subdir).resolve()}")


if __name__ == "__main__":
    main()