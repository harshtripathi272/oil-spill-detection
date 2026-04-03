from __future__ import annotations

import argparse
import csv
import random
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

import cv2
import numpy as np
import tifffile as tiff
from tqdm import tqdm


@dataclass(frozen=True)
class Sample:
    source: str
    image_path: Path
    mask_path: Path
    stem: str
    has_object: bool


def parse_args() -> argparse.Namespace:
    repo_dir = Path(__file__).resolve().parent
    workspace_dir = repo_dir.parent

    parser = argparse.ArgumentParser(
        description=(
            "Build a YOLO segmentation dataset from the oil-spill and no-oil TIFF collections."
        )
    )
    parser.add_argument(
        "--oil-images",
        type=Path,
        default=workspace_dir / "Train_Val_Oil_Spill_images",
        help="Directory with oil-spill TIFF images.",
    )
    parser.add_argument(
        "--oil-masks",
        type=Path,
        default=workspace_dir / "Train_Val_Oil_Spill_masks",
        help="Directory with oil-spill TIFF masks.",
    )
    parser.add_argument(
        "--no-oil-images",
        type=Path,
        default=workspace_dir / "OIL_DATA/01_Train_Val_No_Oil_Images/No_oil",
        help="Directory with no-oil TIFF images.",
    )
    parser.add_argument(
        "--no-oil-masks",
        type=Path,
        default=workspace_dir / "OIL_DATA/01_Train_Val_No_Oil_mask/Mask_no_oil",
        help="Directory with no-oil TIFF masks.",
    )
    parser.add_argument(
        "--lookalike-images",
        type=Path,
        default=workspace_dir / "OIL_DATA/01_Train_Val_Lookalike_images/Lookalike",
        help="Directory with lookalike (false-positive) TIFF images.",
    )
    parser.add_argument(
        "--lookalike-masks",
        type=Path,
        default=workspace_dir / "OIL_DATA/01_Train_Val_Lookalike_mask/Mask_lookalike",
        help="Directory with lookalike (false-positive) TIFF masks.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=repo_dir / "datasets/",
        help="Output dataset directory.",
    )
    parser.add_argument("--val-ratio", type=float, default=0.2, help="Validation split ratio.")
    parser.add_argument("--seed", type=int, default=42, help="Random seed for the split.")
    parser.add_argument(
        "--max-per-source",
        type=int,
        default=None,
        help="Optional limit per source class for quick test runs.",
    )
    parser.add_argument(
        "--min-area",
        type=float,
        default=8.0,
        help="Minimum contour area to keep when converting masks to polygons.",
    )
    parser.add_argument(
        "--approx-epsilon",
        type=float,
        default=0.005,
        help="Contour simplification factor used for segmentation polygons.",
    )
    parser.add_argument(
        "--max-side",
        type=int,
        default=None,
        help="Optional resize target for the longest image side before saving.",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Remove the output directory if it already exists.",
    )
    return parser.parse_args()


def collect_pairs(image_dir: Path, mask_dir: Path, source: str, has_object: bool) -> list[Sample]:
    images = {path.stem: path for path in sorted(image_dir.glob("*.tif"))}
    masks = {path.stem: path for path in sorted(mask_dir.glob("*.tif"))}
    stems = sorted(images.keys() & masks.keys())

    missing_images = sorted(masks.keys() - images.keys())
    missing_masks = sorted(images.keys() - masks.keys())
    if missing_images:
        raise FileNotFoundError(
            f"Found {len(missing_images)} mask file(s) without matching image(s) in {source}: {missing_images[:5]}"
        )
    if missing_masks:
        raise FileNotFoundError(
            f"Found {len(missing_masks)} image file(s) without matching mask(s) in {source}: {missing_masks[:5]}"
        )

    return [
        Sample(source=source, image_path=images[stem], mask_path=masks[stem], stem=stem, has_object=has_object)
        for stem in stems
    ]


def read_tiff(path: Path) -> np.ndarray:
    return tiff.imread(str(path))


def normalize_channel(channel: np.ndarray) -> np.ndarray:
    channel = channel.astype(np.float32)
    low = np.percentile(channel, 1)
    high = np.percentile(channel, 99)
    if not np.isfinite(low) or not np.isfinite(high) or high <= low:
        return np.zeros(channel.shape, dtype=np.uint8)
    clipped = np.clip(channel, low, high)
    scaled = (clipped - low) * 255.0 / (high - low)
    return scaled.astype(np.uint8)


def resize_long_side(image: np.ndarray, max_side: int | None) -> np.ndarray:
    if max_side is None:
        return image
    height, width = image.shape[:2]
    longest = max(height, width)
    if longest <= max_side:
        return image
    scale = max_side / float(longest)
    new_width = max(1, int(round(width * scale)))
    new_height = max(1, int(round(height * scale)))
    return cv2.resize(image, (new_width, new_height), interpolation=cv2.INTER_AREA)


def sar_to_rgb_uint8(array: np.ndarray, max_side: int | None) -> np.ndarray:
    if array.ndim == 2:
        array = array[:, :, None]
    elif array.ndim == 3 and array.shape[0] in {1, 2, 3} and array.shape[-1] not in {1, 2, 3}:
        array = np.moveaxis(array, 0, -1)

    if array.ndim != 3:
        raise ValueError(f"Unsupported SAR image shape: {array.shape}")

    channels = array.shape[2]
    if channels == 1:
        array = np.repeat(array, 3, axis=2)
    elif channels == 2:
        mean_channel = array.mean(axis=2, keepdims=True)
        array = np.concatenate([array, mean_channel], axis=2)
    elif channels > 3:
        array = array[:, :, :3]

    rgb = np.stack([normalize_channel(array[:, :, idx]) for idx in range(3)], axis=2)
    rgb = resize_long_side(rgb, max_side)
    return rgb


def contour_to_yolo_polygon(contour: np.ndarray, width: int, height: int) -> list[float]:
    points = contour.reshape(-1, 2).astype(np.float32)
    if points.shape[0] < 3:
        return []
    points[:, 0] = np.clip(points[:, 0] / float(width), 0.0, 1.0)
    points[:, 1] = np.clip(points[:, 1] / float(height), 0.0, 1.0)
    flattened: list[float] = []
    for x_coord, y_coord in points:
        flattened.extend([float(x_coord), float(y_coord)])
    return flattened


def mask_to_polygons(mask: np.ndarray, min_area: float, approx_epsilon: float) -> list[list[float]]:
    binary = (mask > 0).astype(np.uint8) * 255
    if not np.any(binary):
        return []

    contours_result = cv2.findContours(binary, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
    contours = contours_result[0] if len(contours_result) == 2 else contours_result[1]

    polygons: list[list[float]] = []
    height, width = mask.shape[:2]
    for contour in contours:
        area = cv2.contourArea(contour)
        if area < min_area:
            continue
        epsilon = approx_epsilon * cv2.arcLength(contour, True)
        simplified = cv2.approxPolyDP(contour, epsilon, True)
        polygon = contour_to_yolo_polygon(simplified, width, height)
        if len(polygon) >= 6:
            polygons.append(polygon)
    return polygons


def ensure_clean_output(output_dir: Path, overwrite: bool) -> None:
    if output_dir.exists() and overwrite:
        shutil.rmtree(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    for subdir in [
        output_dir / "images/train",
        output_dir / "images/val",
        output_dir / "labels/train",
        output_dir / "labels/val",
    ]:
        subdir.mkdir(parents=True, exist_ok=True)


def split_samples(samples: list[Sample], val_ratio: float, seed: int) -> tuple[list[Sample], list[Sample]]:
    shuffled = samples[:]
    random.Random(seed).shuffle(shuffled)
    val_count = int(round(len(shuffled) * val_ratio))
    val_count = max(1 if len(shuffled) > 1 else 0, min(val_count, len(shuffled) - 1)) if len(shuffled) > 1 else 0
    return shuffled[val_count:], shuffled[:val_count]


def write_yolo_label(label_path: Path, polygons: Iterable[list[float]]) -> None:
    with label_path.open("w", encoding="utf-8") as handle:
        for polygon in polygons:
            coords = " ".join(f"{value:.6f}" for value in polygon)
            handle.write(f"0 {coords}\n")


def export_sample(sample: Sample, split: str, output_dir: Path, max_side: int | None, min_area: float, approx_epsilon: float) -> tuple[int, int]:
    image = read_tiff(sample.image_path)
    mask = read_tiff(sample.mask_path)

    rgb = sar_to_rgb_uint8(image, max_side=max_side)
    mask = resize_long_side(mask, max_side)

    split_image_dir = output_dir / f"images/{split}"
    split_label_dir = output_dir / f"labels/{split}"
    filename = f"{sample.source}_{sample.stem}.png"
    image_path = split_image_dir / filename
    label_path = split_label_dir / f"{sample.source}_{sample.stem}.txt"

    if not cv2.imwrite(str(image_path), cv2.cvtColor(rgb, cv2.COLOR_RGB2BGR)):
        raise RuntimeError(f"Failed to write image: {image_path}")

    polygons = mask_to_polygons(mask, min_area=min_area, approx_epsilon=approx_epsilon) if sample.has_object else []
    write_yolo_label(label_path, polygons)
    return 1, len(polygons)


def write_dataset_yaml(output_dir: Path) -> None:
    yaml_path = output_dir / "dataset.yaml"
    yaml_text = "\n".join(
        [
            f"path: {output_dir.resolve()}",
            "train: images/train",
            "val: images/val",
            "nc: 1",
            "names:",
            "  - oil_spill",
            "",
        ]
    )
    yaml_path.write_text(yaml_text, encoding="utf-8")


def write_manifest(output_dir: Path, rows: list[dict[str, object]]) -> None:
    manifest_path = output_dir / "manifest.csv"
    with manifest_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=["split", "source", "stem", "image", "mask", "has_object", "polygons"])
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    args = parse_args()

    oil_samples = collect_pairs(args.oil_images, args.oil_masks, source="oil", has_object=True)
    no_oil_samples = collect_pairs(args.no_oil_images, args.no_oil_masks, source="no_oil", has_object=False)
    lookalike_samples = collect_pairs(args.lookalike_images, args.lookalike_masks, source="lookalike", has_object=False)

    if args.max_per_source is not None:
        oil_samples = oil_samples[: args.max_per_source]
        no_oil_samples = no_oil_samples[: args.max_per_source]
        lookalike_samples = lookalike_samples[: args.max_per_source]

    train_oil, val_oil = split_samples(oil_samples, args.val_ratio, args.seed)
    train_no_oil, val_no_oil = split_samples(no_oil_samples, args.val_ratio, args.seed + 1)
    train_lookalike, val_lookalike = split_samples(lookalike_samples, args.val_ratio, args.seed + 2)

    ensure_clean_output(args.output, args.overwrite)

    manifest_rows: list[dict[str, object]] = []
    total_images = 0
    total_polygons = 0

    jobs: list[tuple[str, Sample]] = []
    for split, split_samples_list in [
        ("train", train_oil + train_no_oil + train_lookalike),
        ("val", val_oil + val_no_oil + val_lookalike),
    ]:
        random.Random(args.seed if split == "train" else args.seed + 99).shuffle(split_samples_list)
        jobs.extend((split, sample) for sample in split_samples_list)

    progress = tqdm(jobs, total=len(jobs), unit="img", desc="Building YOLO dataset")
    for split, sample in progress:
        progress.set_postfix(split=split, sample=f"{sample.source}_{sample.stem}", refresh=False)
        image_count, polygon_count = export_sample(
            sample,
            split=split,
            output_dir=args.output,
            max_side=args.max_side,
            min_area=args.min_area,
            approx_epsilon=args.approx_epsilon,
        )
        total_images += image_count
        total_polygons += polygon_count
        manifest_rows.append(
            {
                "split": split,
                "source": sample.source,
                "stem": sample.stem,
                "image": f"images/{split}/{sample.source}_{sample.stem}.png",
                "mask": str(sample.mask_path),
                "has_object": sample.has_object,
                "polygons": polygon_count,
            }
        )

    write_dataset_yaml(args.output)
    write_manifest(args.output, manifest_rows)

    print(f"Output dataset: {args.output.resolve()}")
    print(f"Images written: {total_images}")
    print(f"Oil polygons written: {total_polygons}")
    print(f"Train split: {len(train_oil) + len(train_no_oil) + len(train_lookalike)} (oil: {len(train_oil)}, no-oil: {len(train_no_oil)}, lookalike: {len(train_lookalike)})")
    print(f"Val split: {len(val_oil) + len(val_no_oil) + len(val_lookalike)} (oil: {len(val_oil)}, no-oil: {len(val_no_oil)}, lookalike: {len(val_lookalike)})")
    print(f"YOLO config: {args.output / 'dataset.yaml'}")


if __name__ == "__main__":
    main()