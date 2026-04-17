from __future__ import annotations

import argparse
import shutil
from pathlib import Path

import cv2
import numpy as np


def lee_filter(channel: np.ndarray, kernel_size: int = 9) -> np.ndarray:
    """Adaptive Lee filter for SAR speckle suppression."""
    img = channel.astype(np.float32)
    mean = cv2.boxFilter(img, ddepth=-1, ksize=(kernel_size, kernel_size))
    mean_sq = cv2.boxFilter(img * img, ddepth=-1, ksize=(kernel_size, kernel_size))
    var_local = np.maximum(mean_sq - mean**2, 0.0)

    var_noise = float(np.median(var_local))
    weight = var_local / (var_local + var_noise + 1e-10)
    filtered = mean + weight * (img - mean)
    return filtered.astype(np.float32)


def percentile_normalise(
    channel: np.ndarray, low_pct: float = 1.0, high_pct: float = 99.0
) -> np.ndarray:
    low = np.percentile(channel, low_pct)
    high = np.percentile(channel, high_pct)
    if not (np.isfinite(low) and np.isfinite(high)) or high <= low:
        return np.zeros(channel.shape, dtype=np.uint8)

    clipped = np.clip(channel, low, high)
    scaled = (clipped - low) * 255.0 / (high - low)
    return scaled.astype(np.uint8)


def denoise_channel(
    channel: np.ndarray,
    h: float = 8.0,
    template_window: int = 7,
    search_window: int = 21,
    enable: bool = True,
) -> np.ndarray:
    if not enable or h <= 0:
        return channel

    return cv2.fastNlMeansDenoising(
        channel,
        None,
        h=float(h),
        templateWindowSize=int(template_window),
        searchWindowSize=int(search_window),
    )


def apply_clahe(
    channel: np.ndarray,
    clip_limit: float = 1.8,
    tile_grid: tuple[int, int] = (8, 8),
) -> np.ndarray:
    clahe = cv2.createCLAHE(clipLimit=clip_limit, tileGridSize=tile_grid)
    return clahe.apply(channel)


def apply_border_mask(image: np.ndarray, border_px: int = 8) -> np.ndarray:
    if border_px <= 0:
        return image

    masked = image.copy()
    masked[:border_px, :] = 0
    masked[-border_px:, :] = 0
    masked[:, :border_px] = 0
    masked[:, -border_px:] = 0
    return masked


def resize_to_model_input(
    image: np.ndarray, target: int = 640, interpolation: int = cv2.INTER_AREA
) -> np.ndarray:
    h, w = image.shape[:2]
    scale = target / max(h, w)
    new_w = max(1, int(round(w * scale)))
    new_h = max(1, int(round(h * scale)))
    resized = cv2.resize(image, (new_w, new_h), interpolation=interpolation)

    canvas = np.full((target, target, 3), 114, dtype=np.uint8)
    pad_y = (target - new_h) // 2
    pad_x = (target - new_w) // 2
    canvas[pad_y : pad_y + new_h, pad_x : pad_x + new_w] = resized
    return canvas


def preprocess_sar_png(
    input_path: Path,
    output_path: Path,
    *,
    lee_kernel: int = 9,
    border_px: int = 8,
    clahe_clip: float = 1.8,
    clahe_tile: int = 8,
    denoise_enable: bool = True,
    denoise_h: float = 8.0,
    denoise_template: int = 7,
    denoise_search: int = 21,
    model_input: int = 640,
) -> np.ndarray:
    bgr = cv2.imread(str(input_path), cv2.IMREAD_COLOR)
    if bgr is None:
        raise FileNotFoundError(f"Cannot read image: {input_path}")

    rgb = cv2.cvtColor(bgr, cv2.COLOR_BGR2RGB)

    ch_vv = rgb[:, :, 0].astype(np.float32)
    ch_vh = rgb[:, :, 1].astype(np.float32)
    ch_mean = rgb[:, :, 2].astype(np.float32)

    def to_db_like(c: np.ndarray) -> np.ndarray:
        c_lin = c / 255.0 + 1e-6
        return 10.0 * np.log10(c_lin)

    ch_vv_lee = lee_filter(to_db_like(ch_vv), kernel_size=lee_kernel)
    ch_vh_lee = lee_filter(to_db_like(ch_vh), kernel_size=lee_kernel)
    ch_mean_lee = lee_filter(to_db_like(ch_mean), kernel_size=lee_kernel)

    ch_vv_norm = percentile_normalise(ch_vv_lee)
    ch_vh_norm = percentile_normalise(ch_vh_lee)
    ch_mean_norm = percentile_normalise(ch_mean_lee)

    ch_vv_dn = denoise_channel(
        ch_vv_norm,
        h=denoise_h,
        template_window=denoise_template,
        search_window=denoise_search,
        enable=denoise_enable,
    )
    ch_vh_dn = denoise_channel(
        ch_vh_norm,
        h=denoise_h,
        template_window=denoise_template,
        search_window=denoise_search,
        enable=denoise_enable,
    )
    ch_mean_dn = denoise_channel(
        ch_mean_norm,
        h=denoise_h,
        template_window=denoise_template,
        search_window=denoise_search,
        enable=denoise_enable,
    )

    tile = (clahe_tile, clahe_tile)
    rgb_proc = np.stack(
        [
            apply_clahe(ch_vv_dn, clip_limit=clahe_clip, tile_grid=tile),
            apply_clahe(ch_vh_dn, clip_limit=clahe_clip, tile_grid=tile),
            apply_clahe(ch_mean_dn, clip_limit=clahe_clip, tile_grid=tile),
        ],
        axis=2,
    )

    rgb_proc = apply_border_mask(rgb_proc, border_px=border_px)
    rgb_model = resize_to_model_input(rgb_proc, target=model_input)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    bgr_out = cv2.cvtColor(rgb_model, cv2.COLOR_RGB2BGR)
    cv2.imwrite(str(output_path), bgr_out)
    return bgr_out


def _iter_images(input_dir: Path) -> list[Path]:
    exts = {".png", ".jpg", ".jpeg"}
    return sorted(
        p for p in input_dir.rglob("*") if p.is_file() and p.suffix.lower() in exts
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Batch preprocess SAR images for oil-spill detection."
    )
    parser.add_argument(
        "--input-dir",
        type=Path,
        default=Path("datasets/images"),
        help="Root folder containing SAR images.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("datasets/images_preprocessed"),
        help="Output folder for preprocessed images.",
    )
    parser.add_argument("--lee-kernel", type=int, default=9)
    parser.add_argument("--border-px", type=int, default=8)
    parser.add_argument("--clahe-clip", type=float, default=1.8)
    parser.add_argument("--clahe-tile", type=int, default=8)
    parser.add_argument("--denoise-h", type=float, default=8.0)
    parser.add_argument("--denoise-template", type=int, default=7)
    parser.add_argument("--denoise-search", type=int, default=21)
    parser.add_argument("--model-input", type=int, default=1024)
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Delete output directory before preprocessing.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    if args.lee_kernel % 2 == 0:
        raise ValueError("--lee-kernel must be odd.")
    if args.denoise_template % 2 == 0 or args.denoise_search % 2 == 0:
        raise ValueError("--denoise-template and --denoise-search must be odd.")

    input_dir = args.input_dir.resolve()
    output_dir = args.output_dir.resolve()

    if output_dir.exists() and args.overwrite:
        shutil.rmtree(output_dir)

    images = _iter_images(input_dir)
    if not images:
        raise FileNotFoundError(f"No images found under: {input_dir}")

    print(f"Found {len(images)} images under {input_dir}")
    for idx, in_path in enumerate(images, start=1):
        rel = in_path.relative_to(input_dir)
        out_path = output_dir / rel
        preprocess_sar_png(
            input_path=in_path,
            output_path=out_path,
            lee_kernel=args.lee_kernel,
            border_px=args.border_px,
            clahe_clip=args.clahe_clip,
            clahe_tile=args.clahe_tile,
            denoise_enable=args.denoise_h > 0,
            denoise_h=args.denoise_h,
            denoise_template=args.denoise_template,
            denoise_search=args.denoise_search,
            model_input=args.model_input,
        )
        if idx % 100 == 0 or idx == len(images):
            print(f"Processed {idx}/{len(images)}")

    print(f"Done. Preprocessed images saved to {output_dir}")


if __name__ == "__main__":
    main()
