"""
Pipeline visibility endpoints.
Serves DAG structure, SAR images, YOLO model results, anomaly stats,
and a confidence-score histogram — all from files on disk, no DB required.
"""

import csv
import os
import re
from collections import Counter
from datetime import datetime
from fastapi import APIRouter
from typing import List, Dict, Any

router = APIRouter()

BASE_DIR = "/data/user13/oilspill_ugq/oil-spill-detection"
LOGS_DIR = os.path.join(BASE_DIR, "logs")
SAR_DIR = os.path.join(BASE_DIR, "sentinel_data", "preprocessed")
YOLO_RESULTS = os.path.join(
    BASE_DIR, "runs", "yolo", "yolo26n-bbox-1024-merged", "results.csv"
)


@router.get("/dag-flow")
async def get_dag_flow():
    """Return the suspicious-event DAG task chain as JSON."""
    return {
        "dag_id": "suspicious_event_validation",
        "description": "Validates suspicious AIS events using Sentinel-1 imagery",
        "tasks": [
            {
                "id": "wait_for_sar_trigger",
                "label": "Kafka Trigger Sensor",
                "description": "Listens for SAR trigger events on Kafka topic",
                "type": "sensor",
            },
            {
                "id": "initialize_incident",
                "label": "Initialize Incident",
                "description": "Creates incident state from trigger payload",
                "type": "python",
            },
            {
                "id": "prepare_search_params",
                "label": "Prepare Search Params",
                "description": "Calculates ROI bounding box and date range",
                "type": "python",
            },
            {
                "id": "search_sentinel",
                "label": "Search Sentinel-1",
                "description": "Queries ASF for overlapping SAR granules",
                "type": "operator",
            },
            {
                "id": "download_sentinel",
                "label": "Download SAR Data",
                "description": "Downloads HDF5 data from ASF",
                "type": "operator",
            },
            {
                "id": "sar_inference",
                "label": "YOLO Oil-Spill Inference",
                "description": "Runs YOLOv26n detection on preprocessed SAR tiles",
                "type": "operator",
            },
            {
                "id": "finalize_incident",
                "label": "Finalize Incident",
                "description": "Updates incident state based on inference results",
                "type": "python",
            },
        ],
    }


@router.get("/sar-images")
async def get_sar_images():
    """List available preprocessed SAR images."""
    if not os.path.isdir(SAR_DIR):
        return {"images": []}

    images: List[Dict[str, Any]] = []
    for fname in sorted(os.listdir(SAR_DIR)):
        if not fname.endswith(".png"):
            continue
        filepath = os.path.join(SAR_DIR, fname)
        stat = os.stat(filepath)
        # Parse granule ID and type from filename
        is_preprocessed = "_preprocessed" in fname
        granule_id = fname.replace("_preprocessed.png", "").replace("_raw.png", "")
        images.append(
            {
                "filename": fname,
                "granule_id": granule_id,
                "type": "preprocessed" if is_preprocessed else "raw",
                "size": stat.st_size,
                "modified": datetime.fromtimestamp(stat.st_mtime).isoformat(),
                "url": f"/sar-images/{fname}",
            }
        )
    return {"images": images, "total": len(images)}


@router.get("/model-results")
async def get_model_results():
    """Return YOLO training results from the best run."""
    if not os.path.isfile(YOLO_RESULTS):
        return {"error": "No training results found", "epochs": []}

    epochs = []
    with open(YOLO_RESULTS, "r") as f:
        reader = csv.DictReader(f)
        for row in reader:
            cleaned = {}
            for k, v in row.items():
                key = k.strip()
                try:
                    cleaned[key] = round(float(v.strip()), 5)
                except (ValueError, AttributeError):
                    cleaned[key] = v.strip() if isinstance(v, str) else v
            epochs.append(cleaned)

    # Extract key final metrics
    final = epochs[-1] if epochs else {}
    return {
        "run_name": "yolo26n-bbox-1024-merged",
        "total_epochs": len(epochs),
        "final_metrics": {
            "mAP50": final.get("metrics/mAP50(B)"),
            "mAP50_95": final.get("metrics/mAP50-95(B)"),
            "precision": final.get("metrics/precision(B)"),
            "recall": final.get("metrics/recall(B)"),
        },
        "epochs": epochs,
    }


@router.get("/anomaly-stats")
async def get_anomaly_stats():
    """Parse anomaly_detector.log to extract summary statistics."""
    log_path = os.path.join(LOGS_DIR, "anomaly_detector.log")
    if not os.path.isfile(log_path):
        return {"error": "Log file not found"}

    anomaly_pattern = re.compile(
        r"\[ANOMALY DETECTED\] Vessel: (\d+), Score: ([\d.]+), "
        r"Lat: ([-\d.]+), Lon: ([-\d.]+)"
    )

    total_lines = 0
    anomaly_count = 0
    vessels: Counter = Counter()
    scores: list = []
    recent_anomalies: list = []

    with open(log_path, "r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            total_lines += 1
            m = anomaly_pattern.search(line)
            if m:
                anomaly_count += 1
                vessel_id = m.group(1)
                score = float(m.group(2))
                lat = float(m.group(3))
                lon = float(m.group(4))
                vessels[vessel_id] += 1
                scores.append(score)

                # Grab timestamp
                ts_match = re.match(r"^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})", line)
                ts = ts_match.group(1) if ts_match else ""

                recent_anomalies.append(
                    {
                        "vessel": vessel_id,
                        "score": score,
                        "lat": lat,
                        "lon": lon,
                        "timestamp": ts,
                    }
                )

    avg_score = round(sum(scores) / len(scores), 4) if scores else 0
    top_vessels = vessels.most_common(10)

    return {
        "total_log_lines": total_lines,
        "total_anomalies": anomaly_count,
        "unique_vessels": len(vessels),
        "avg_anomaly_score": avg_score,
        "top_vessels": [{"vessel_id": v, "count": c} for v, c in top_vessels],
        "recent_anomalies": recent_anomalies[-20:],  # last 20
    }


@router.get("/confidence-histogram")
async def get_confidence_histogram():
    """
    Returns a histogram of anomaly scores from the detector log,
    bucketed into 0.1-wide bins (0.6-0.7, 0.7-0.8, 0.8-0.9, 0.9-1.0).
    Used as the 'Incident Trends' replacement chart on the dashboard.
    """
    log_path = os.path.join(LOGS_DIR, "anomaly_detector.log")
    if not os.path.isfile(log_path):
        return {"labels": [], "counts": [], "total": 0}

    score_pattern = re.compile(r"Score: ([\d.]+)")
    bins = {"0.60-0.70": 0, "0.70-0.80": 0, "0.80-0.90": 0, "0.90-1.00": 0}

    total = 0
    with open(log_path, "r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            m = score_pattern.search(line)
            if m:
                score = float(m.group(1))
                total += 1
                if score < 0.70:
                    bins["0.60-0.70"] += 1
                elif score < 0.80:
                    bins["0.70-0.80"] += 1
                elif score < 0.90:
                    bins["0.80-0.90"] += 1
                else:
                    bins["0.90-1.00"] += 1

    return {
        "labels": list(bins.keys()),
        "counts": list(bins.values()),
        "total": total,
    }
