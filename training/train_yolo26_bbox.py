from __future__ import annotations

import argparse
import json
import os
import random
import shutil
from pathlib import Path

import albumentations as A
import cv2
import wandb
import yaml
from ultralytics import YOLO

ROOT = Path(__file__).resolve().parents[1]


def resolve_images_root(images_root: Path, prefer_preprocessed: bool) -> Path:
    """Choose images_preprocessed when requested and available."""
    candidate = (ROOT / "datasets" / "images_preprocessed").resolve()
    chosen = images_root.resolve()
    if prefer_preprocessed and candidate.exists():
        print(f"Using preprocessed images: {candidate}")
        return candidate

    if prefer_preprocessed and not candidate.exists():
        print(
            "images_preprocessed not found; using provided images root instead: "
            f"{chosen}"
        )
    return chosen


def parse_label_file(label_path: Path) -> list[list[float]]:
    if not label_path.exists():
        return []

    records: list[list[float]] = []
    for line in label_path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        parts = line.split()
        if len(parts) != 5:
            continue
        cls_id = int(float(parts[0]))
        x, y, w, h = [float(v) for v in parts[1:]]
        records.append([cls_id, x, y, w, h])
    return records


def write_label_file(label_path: Path, records: list[list[float]]) -> None:
    label_path.parent.mkdir(parents=True, exist_ok=True)
    lines = [f"{int(r[0])} {r[1]:.6f} {r[2]:.6f} {r[3]:.6f} {r[4]:.6f}" for r in records]
    label_path.write_text("\n".join(lines), encoding="utf-8")


def _find_image_for_stem(image_dir: Path, stem: str) -> Path | None:
    for ext in (".png", ".jpg", ".jpeg"):
        p = image_dir / f"{stem}{ext}"
        if p.exists():
            return p
    return None


def _copy_split(
    image_src: Path,
    label_src: Path,
    image_dst: Path,
    label_dst: Path,
) -> int:
    image_dst.mkdir(parents=True, exist_ok=True)
    label_dst.mkdir(parents=True, exist_ok=True)

    copied = 0
    for lbl in sorted(label_src.glob("*.txt")):
        stem = lbl.stem
        img = _find_image_for_stem(image_src, stem)
        if img is None:
            continue
        shutil.copy2(img, image_dst / img.name)
        shutil.copy2(lbl, label_dst / lbl.name)
        copied += 1
    return copied


def build_augmentation_pipeline() -> A.Compose:
    return A.Compose(
        [
            A.HorizontalFlip(p=0.5),
            A.ShiftScaleRotate(
                shift_limit=0.08,
                scale_limit=0.15,
                rotate_limit=12,
                border_mode=cv2.BORDER_REFLECT_101,
                p=0.7,
            ),
            A.RandomBrightnessContrast(
                brightness_limit=0.15,
                contrast_limit=0.15,
                p=0.5,
            ),
            A.GaussNoise(std_range=(0.01, 0.03), p=0.3),
            A.MotionBlur(blur_limit=(3, 5), p=0.2),
        ],
        bbox_params=A.BboxParams(
            format="yolo",
            label_fields=["class_labels"],
            min_visibility=0.2,
            clip=True,
        ),
    )


def augment_train_split(
    train_images_dir: Path,
    train_labels_dir: Path,
    out_images_dir: Path,
    out_labels_dir: Path,
    aug_per_image: int,
    seed: int,
) -> tuple[int, int]:
    random.seed(seed)
    transform = build_augmentation_pipeline()

    out_images_dir.mkdir(parents=True, exist_ok=True)
    out_labels_dir.mkdir(parents=True, exist_ok=True)

    originals = 0
    augmentations = 0

    for lbl in sorted(train_labels_dir.glob("*.txt")):
        stem = lbl.stem
        img_path = _find_image_for_stem(train_images_dir, stem)
        if img_path is None:
            continue

        bgr = cv2.imread(str(img_path), cv2.IMREAD_COLOR)
        if bgr is None:
            continue
        image = cv2.cvtColor(bgr, cv2.COLOR_BGR2RGB)

        records = parse_label_file(lbl)
        class_labels = [int(r[0]) for r in records]
        bboxes = [[r[1], r[2], r[3], r[4]] for r in records]

        # Keep original sample
        shutil.copy2(img_path, out_images_dir / img_path.name)
        write_label_file(out_labels_dir / lbl.name, records)
        originals += 1

        if not bboxes:
            continue

        for i in range(aug_per_image):
            transformed = transform(
                image=image,
                bboxes=bboxes,
                class_labels=class_labels,
            )
            aug_img = transformed["image"]
            aug_boxes = transformed["bboxes"]
            aug_classes = transformed["class_labels"]

            if not aug_boxes:
                continue

            out_name = f"{stem}_aug{i:02d}{img_path.suffix.lower()}"
            out_lbl_name = f"{stem}_aug{i:02d}.txt"

            out_bgr = cv2.cvtColor(aug_img, cv2.COLOR_RGB2BGR)
            cv2.imwrite(str(out_images_dir / out_name), out_bgr)

            out_records = [
                [int(c), float(b[0]), float(b[1]), float(b[2]), float(b[3])]
                for b, c in zip(aug_boxes, aug_classes)
            ]
            write_label_file(out_labels_dir / out_lbl_name, out_records)
            augmentations += 1

    return originals, augmentations


def write_dataset_yaml(dataset_root: Path, yaml_path: Path) -> None:
    data = {
        "path": str(dataset_root.resolve()),
        "train": "images/train",
        "val": "images/val",
        "nc": 1,
        "names": ["oil_spill"],
    }
    yaml_path.parent.mkdir(parents=True, exist_ok=True)
    with yaml_path.open("w", encoding="utf-8") as f:
        yaml.safe_dump(data, f, sort_keys=False)


def prepare_detection_dataset(
    source_images_root: Path,
    source_labels_bbox_root: Path,
    out_root: Path,
    aug_per_image: int,
    seed: int,
) -> Path:
    train_images_src = source_images_root / "train"
    val_images_src = source_images_root / "val"
    train_labels_src = source_labels_bbox_root / "train"
    val_labels_src = source_labels_bbox_root / "val"

    if not train_images_src.exists() or not val_images_src.exists():
        raise FileNotFoundError(f"Missing images train/val under: {source_images_root}")
    if not train_labels_src.exists() or not val_labels_src.exists():
        raise FileNotFoundError(f"Missing labels_bbox train/val under: {source_labels_bbox_root}")

    if out_root.exists():
        shutil.rmtree(out_root)

    out_images_train = out_root / "images" / "train"
    out_images_val = out_root / "images" / "val"
    out_labels_train = out_root / "labels" / "train"
    out_labels_val = out_root / "labels" / "val"

    originals, augmented = augment_train_split(
        train_images_dir=train_images_src,
        train_labels_dir=train_labels_src,
        out_images_dir=out_images_train,
        out_labels_dir=out_labels_train,
        aug_per_image=aug_per_image,
        seed=seed,
    )

    val_copied = _copy_split(
        image_src=val_images_src,
        label_src=val_labels_src,
        image_dst=out_images_val,
        label_dst=out_labels_val,
    )

    dataset_yaml = out_root / "dataset_bbox_aug.yaml"
    write_dataset_yaml(out_root, dataset_yaml)

    print(f"Prepared dataset at: {out_root}")
    print(f"Train originals copied: {originals}")
    print(f"Train augmentations created: {augmented}")
    print(f"Val pairs copied: {val_copied}")
    print(f"Dataset yaml: {dataset_yaml}")

    return dataset_yaml


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Prepare augmented bbox dataset and train YOLOv26 for oil-spill detection."
    )
    parser.add_argument(
        "--images-root",
        type=Path,
        default=ROOT / "datasets" / "images",
        help="Input images root with train/ and val/ folders.",
    )
    parser.add_argument(
        "--prefer-preprocessed",
        action="store_true",
        help=(
            "If datasets/images_preprocessed exists, use it instead of --images-root. "
            "Run preprocessing script first to create that directory."
        ),
    )
    parser.add_argument(
        "--labels-bbox-root",
        type=Path,
        default=ROOT / "datasets" / "labels_bbox",
        help="YOLO-format bbox labels root with train/ and val/ folders.",
    )
    parser.add_argument(
        "--prepared-dataset-root",
        type=Path,
        default=ROOT / "datasets" / "bbox_augmented",
        help="Output root where augmented detection dataset will be built.",
    )
    parser.add_argument(
        "--model",
        type=str,
        default=str((ROOT.parent / "yolo26n.pt").resolve()),
        help="YOLOv26 model path or model name.",
    )
    parser.add_argument("--epochs", type=int, default=100)
    parser.add_argument("--imgsz", type=int, default=1024)
    parser.add_argument("--batch", type=int, default=16)
    parser.add_argument("--workers", type=int, default=8)
    parser.add_argument("--device", type=str, default="0")
    parser.add_argument("--lr0", type=float, default=1e-3)
    parser.add_argument("--weight-decay", type=float, default=5e-4)
    parser.add_argument("--optimizer", type=str, default="AdamW")
    parser.add_argument("--project", type=Path, default=ROOT / "runs" / "yolo")
    parser.add_argument("--name", type=str, default="yolo26n-bbox-albumentations")
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--wandb-project", type=str, default="oilspill")
    parser.add_argument("--wandb-entity", type=str, default=None)
    parser.add_argument(
        "--wandb-mode",
        type=str,
        default="online",
        choices=["online", "offline", "disabled"],
    )
    parser.add_argument(
        "--aug-per-image",
        type=int,
        default=2,
        help="How many augmented variants to generate per training image.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    selected_images_root = resolve_images_root(
        images_root=args.images_root,
        prefer_preprocessed=args.prefer_preprocessed,
    )

    os.environ["WANDB_PROJECT"] = args.wandb_project
    if args.wandb_entity:
        os.environ["WANDB_ENTITY"] = args.wandb_entity
    os.environ["WANDB_MODE"] = args.wandb_mode

    run = wandb.init(
        project=args.wandb_project,
        entity=args.wandb_entity,
        mode=args.wandb_mode,
        name=args.name,
        config={
            "model": args.model,
            "epochs": args.epochs,
            "imgsz": args.imgsz,
            "batch": args.batch,
            "optimizer": args.optimizer,
            "lr0": args.lr0,
            "weight_decay": args.weight_decay,
            "aug_per_image": args.aug_per_image,
            "images_root": str(selected_images_root),
            "labels_bbox_root": str(args.labels_bbox_root),
            "prefer_preprocessed": args.prefer_preprocessed,
        },
        reinit=True,
    )

    dataset_yaml = prepare_detection_dataset(
        source_images_root=selected_images_root,
        source_labels_bbox_root=args.labels_bbox_root,
        out_root=args.prepared_dataset_root,
        aug_per_image=max(0, args.aug_per_image),
        seed=args.seed,
    )

    model = YOLO(args.model)
    train_result = model.train(
        data=str(dataset_yaml),
        epochs=args.epochs,
        imgsz=args.imgsz,
        batch=args.batch,
        workers=args.workers,
        device=args.device,
        optimizer=args.optimizer,
        lr0=args.lr0,
        weight_decay=args.weight_decay,
        project=str(args.project),
        name=args.name,
        seed=args.seed,
        pretrained=True,
        # Keep built-in YOLO augmentations moderate since we already did offline Albumentations.
        mosaic=0.3,
        mixup=0.0,
        copy_paste=0.0,
        hsv_h=0.01,
        hsv_s=0.4,
        hsv_v=0.3,
        fliplr=0.5,
        patience=20,
        verbose=True,
    )

    # Run explicit validation and log key detection metrics to W&B.
    val_result = model.val(
        data=str(dataset_yaml),
        imgsz=args.imgsz,
        batch=args.batch,
        workers=args.workers,
        device=args.device,
        split="val",
        verbose=False,
    )

    train_metrics: dict[str, float] = {}
    val_metrics: dict[str, float] = {}
    if hasattr(train_result, "results_dict") and train_result.results_dict is not None:
        train_metrics = {
            str(k): float(v)
            for k, v in train_result.results_dict.items()
            if isinstance(v, (float, int))
        }
    if hasattr(val_result, "results_dict") and val_result.results_dict is not None:
        val_metrics = {
            str(k): float(v)
            for k, v in val_result.results_dict.items()
            if isinstance(v, (float, int))
        }

    if train_metrics:
        wandb.log({f"train/{k}": v for k, v in train_metrics.items()})
    if val_metrics:
        wandb.log({f"val/{k}": v for k, v in val_metrics.items()})

    # Surface common metrics in summary for quick experiment comparison.
    for k in (
        "metrics/mAP50(B)",
        "metrics/mAP50-95(B)",
        "metrics/precision(B)",
        "metrics/recall(B)",
        "fitness",
    ):
        if k in val_metrics:
            wandb.summary[f"val/{k}"] = val_metrics[k]

    artifact = wandb.Artifact(name=f"{run.name}-weights", type="model")
    run_dir = Path(args.project) / args.name
    best = run_dir / "weights" / "best.pt"
    last = run_dir / "weights" / "last.pt"
    if best.exists():
        artifact.add_file(str(best))
    if last.exists():
        artifact.add_file(str(last))
    if best.exists() or last.exists():
        wandb.log_artifact(artifact)

    print("Validation metrics:")
    print(json.dumps(val_metrics, indent=2))
    wandb.finish()


if __name__ == "__main__":
    main()
