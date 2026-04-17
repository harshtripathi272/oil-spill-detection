import os
import cv2
import argparse
from pathlib import Path
import random
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
    return filtered

def apply_clahe(channel: np.ndarray, clip_limit: float = 1.8, tile_grid: tuple[int, int] = (8, 8)) -> np.ndarray:
    clahe = cv2.createCLAHE(clipLimit=clip_limit, tileGridSize=tile_grid)
    return clahe.apply(channel)

def preprocess_image(img):
    """Apply SAR specific preprocessing: dB scaling, Lee filter, Denoising, and CLAHE."""
    processed_channels = []
    for i in range(3):
        ch = img[:, :, i].astype(np.float32)
        
        # 1. to_db_like scaling
        ch_lin = ch / 255.0 + 1e-6
        ch_db = 10.0 * np.log10(ch_lin)
        
        # 2. Lee Filter
        ch_lee = lee_filter(ch_db, kernel_size=9)
        
        # 3. Percentile normalization
        low, high = np.percentile(ch_lee, (1, 99))
        if high > low:
            ch_norm = np.clip(ch_lee, low, high)
            ch_norm = ((ch_norm - low) * 255.0 / (high - low)).astype(np.uint8)
        else:
            ch_norm = np.zeros_like(ch, dtype=np.uint8)
            
        # 4. Noise Removal
        ch_dn = cv2.fastNlMeansDenoising(ch_norm, None, h=8, templateWindowSize=7, searchWindowSize=21)
        
        # 5. CLAHE
        ch_final = apply_clahe(ch_dn, clip_limit=1.8, tile_grid=(8, 8))
        processed_channels.append(ch_final)
        
    return np.stack(processed_channels, axis=2)

def parse_polygon_to_bbox(parts):
    """Convert YOLO polygon normalized coords to bbox normalized coords."""
    c_id = int(parts[0])
    coords = [float(p) for p in parts[1:]]
    xs = coords[0::2]
    ys = coords[1::2]
    if not xs or not ys:
        return None
    xmin, xmax = min(xs), max(xs)
    ymin, ymax = min(ys), max(ys)
    return (c_id, xmin, ymin, xmax, ymax)

def slice_image_and_labels(img_path, label_path, out_img_dir, out_lbl_dir, 
                           slice_size=640, overlap=0.2, min_area=100, empty_keep_ratio=0.1, preprocess=True):
    img = cv2.imread(str(img_path))
    if img is None:
        return
    
    if preprocess:
        img = preprocess_image(img)
        
    h, w = img.shape[:2]
    bboxes = []
    if label_path.exists():
        with open(label_path, 'r') as f:
            for line in f:
                parts = line.strip().split()
                if len(parts) >= 5: 
                    bbox = parse_polygon_to_bbox(parts)
                    if bbox is not None:
                        c_id, xmin, ymin, xmax, ymax = bbox
                        abs_x1, abs_y1 = xmin * w, ymin * h
                        abs_x2, abs_y2 = xmax * w, ymax * h
                        if abs_x2 > abs_x1 and abs_y2 > abs_y1:
                            bboxes.append((c_id, abs_x1, abs_y1, abs_x2, abs_y2))
                            
    stride = int(slice_size * (1 - overlap))
    patch_idx = 0
    y_starts = list(range(0, h, stride))
    x_starts = list(range(0, w, stride))
    if not y_starts or y_starts[-1] + slice_size < h:
        y_starts.append(max(0, h - slice_size))
    if not x_starts or x_starts[-1] + slice_size < w:
        x_starts.append(max(0, w - slice_size))
    y_starts = sorted(list(set(y_starts)))
    x_starts = sorted(list(set(x_starts)))
    
    for py1 in y_starts:
        for px1 in x_starts:
            px2 = min(px1 + slice_size, w)
            py2 = min(py1 + slice_size, h)
            if px2 - px1 < slice_size and w >= slice_size:
                px1 = px2 - slice_size
            if py2 - py1 < slice_size and h >= slice_size:
                py1 = py2 - slice_size
            patch_w, patch_h = px2 - px1, py2 - py1
            if patch_w <= 0 or patch_h <= 0:
                continue

            patch_bboxes = []
            for (c_id, bx1, by1, bx2, by2) in bboxes:
                ix1, iy1 = max(px1, bx1), max(py1, by1)
                ix2, iy2 = min(px2, bx2), min(py2, by2)
                if ix2 > ix1 and iy2 > iy1:
                    area = (ix2 - ix1) * (iy2 - iy1)
                    if area >= min_area:
                        ncx = ((ix1 + ix2) / 2.0 - px1) / patch_w
                        ncy = ((iy1 + iy2) / 2.0 - py1) / patch_h
                        nw = (ix2 - ix1) / patch_w
                        nh = (iy2 - iy1) / patch_h
                        patch_bboxes.append(f"{c_id} {ncx:.6f} {ncy:.6f} {nw:.6f} {nh:.6f}")
                        
            patch_name = f"{img_path.stem}_{patch_idx}"
            out_img_path = out_img_dir / f"{patch_name}.png"
            out_lbl_path = out_lbl_dir / f"{patch_name}.txt"
            
            if len(patch_bboxes) == 0:
                if random.random() < empty_keep_ratio:
                    patch = img[py1:py2, px1:px2]
                    cv2.imwrite(str(out_img_path), patch)
                    open(out_lbl_path, 'w').close()
            else:
                patch = img[py1:py2, px1:px2]
                cv2.imwrite(str(out_img_path), patch)
                with open(out_lbl_path, 'w') as f:
                    f.write("\n".join(patch_bboxes))
            patch_idx += 1

def main():
    parser = argparse.ArgumentParser(description="Slice HD images into YOLO bounding box patches.")
    parser.add_argument("--src", type=str, default="datasets")
    parser.add_argument("--dst", type=str, default="datasets_sliced")
    parser.add_argument("--slice-size", type=int, default=640)
    parser.add_argument("--overlap", type=float, default=0.2)
    parser.add_argument("--empty-ratio", type=float, default=0.1)
    parser.add_argument("--no-preprocess", action="store_true")
    args = parser.parse_args()
    
    src = Path(args.src)
    dst = Path(args.dst)
    random.seed(42)
    
    for split in ["train", "val"]:
        img_dir, lbl_dir = src / "images" / split, src / "labels" / split
        out_img_dir, out_lbl_dir = dst / "images" / split, dst / "labels" / split
        out_img_dir.mkdir(parents=True, exist_ok=True)
        out_lbl_dir.mkdir(parents=True, exist_ok=True)
        
        img_paths = list(img_dir.glob("*.png")) + list(img_dir.glob("*.jpg"))
        print(f"Slicing & Preprocessing {len(img_paths)} images for '{split}' split...")
        for i, ip in enumerate(img_paths):
            lp = lbl_dir / f"{ip.stem}.txt"
            slice_image_and_labels(
                ip, lp, out_img_dir, out_lbl_dir, 
                slice_size=args.slice_size, 
                overlap=args.overlap, 
                empty_keep_ratio=args.empty_ratio,
                preprocess=not args.no_preprocess
            )
            if (i+1) % 100 == 0:
                print(f" => Processed {i+1} / {len(img_paths)}")
                
    yaml_path = dst / "data.yaml"
    yaml_content = f"path: {dst.resolve()}\ntrain: images/train\nval: images/val\nnc: 1\nnames: ['oil_spill']\n"
    with open(yaml_path, "w") as f:
        f.write(yaml_content)

if __name__ == "__main__":
    main()
