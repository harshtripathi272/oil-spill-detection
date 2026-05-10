"""
Analytics Intelligence Service.

Computes operational metrics from existing Incident and Prediction data
without introducing new database tables. All data is derived from the
incidents.extra_metadata JSON, confidence_score, detection_time, and
the predictions table.
"""

import json
import math
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from sqlalchemy import func, extract, case, desc
from sqlalchemy.orm import Session

from app.models.incident import Incident, DagRun
from app.models.predictions import Prediction


def _parse_iso_datetime(value: str) -> Optional[datetime]:
    """Parse metadata / DB datetime strings; normalize trailing Z for fromisoformat."""
    if not value or not isinstance(value, str):
        return None
    v = value.strip()
    if v.endswith("Z"):
        v = v[:-1] + "+00:00"
    try:
        return datetime.fromisoformat(v)
    except (ValueError, TypeError):
        return None


def _to_utc_aware(dt: datetime) -> datetime:
    """Make datetimes comparable (DB / JSON mix of naive and aware)."""
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


class AnalyticsService:
    def __init__(self, db: Session):
        self.db = db

    # ── Detection Trends (weekly / monthly) ─────────────────────────

    def get_detection_trends(self, period: str = "weekly", weeks: int = 12) -> Dict[str, Any]:
        """
        Returns incident counts grouped by week or month.
        period: 'weekly' or 'monthly'
        """
        cutoff = datetime.utcnow() - timedelta(weeks=weeks)
        incidents = (
            self.db.query(Incident)
            .filter(Incident.detection_time >= cutoff)
            .order_by(Incident.detection_time)
            .all()
        )

        buckets: Dict[str, int] = defaultdict(int)
        for inc in incidents:
            dt = inc.detection_time
            if dt is None:
                continue
            if isinstance(dt, str):
                try:
                    dt = datetime.fromisoformat(dt.rstrip("Z"))
                except ValueError:
                    continue
            if period == "monthly":
                key = dt.strftime("%Y-%m")
            else:
                # ISO week: "2026-W19"
                key = f"{dt.isocalendar()[0]}-W{dt.isocalendar()[1]:02d}"
            buckets[key] += 1

        labels = sorted(buckets.keys())
        counts = [buckets[k] for k in labels]

        return {"period": period, "labels": labels, "counts": counts}

    # ── Peak Detection Hours ────────────────────────────────────────

    def get_peak_hours(self) -> Dict[str, Any]:
        """Hour-of-day histogram (0-23) of incident detections."""
        incidents = self.db.query(Incident).filter(
            Incident.detection_time.isnot(None)
        ).all()

        hour_counts = [0] * 24
        for inc in incidents:
            dt = inc.detection_time
            if isinstance(dt, str):
                try:
                    dt = datetime.fromisoformat(dt.rstrip("Z"))
                except ValueError:
                    continue
            if dt is not None:
                hour_counts[dt.hour] += 1

        return {
            "labels": [f"{h:02d}:00" for h in range(24)],
            "counts": hour_counts,
            "peak_hour": hour_counts.index(max(hour_counts)) if any(hour_counts) else None,
        }

    # ── Regional Spill Density ──────────────────────────────────────

    def get_regional_density(self) -> Dict[str, Any]:
        """
        Groups incidents into named ocean regions based on coordinates.
        Returns counts and percentages.
        """
        incidents = self.db.query(Incident).filter(
            Incident.latitude.isnot(None),
            Incident.longitude.isnot(None),
        ).all()

        regions: Dict[str, int] = {
            "North Atlantic": 0,
            "South Atlantic": 0,
            "North Pacific": 0,
            "South Pacific": 0,
            "Indian Ocean": 0,
            "Mediterranean": 0,
            "Arctic": 0,
            "Other": 0,
        }

        for inc in incidents:
            lat, lon = inc.latitude, inc.longitude
            if lat is None or lon is None:
                continue
            if lat > 66:
                regions["Arctic"] += 1
            elif 30 <= lat <= 45 and -6 <= lon <= 36:
                regions["Mediterranean"] += 1
            elif lat >= 0 and -80 <= lon <= 0:
                regions["North Atlantic"] += 1
            elif lat < 0 and -80 <= lon <= 20:
                regions["South Atlantic"] += 1
            elif lat >= 0 and (lon > 100 or lon < -80):
                regions["North Pacific"] += 1
            elif lat < 0 and (lon > 100 or lon < -80):
                regions["South Pacific"] += 1
            elif 20 <= lon <= 100:
                regions["Indian Ocean"] += 1
            else:
                regions["Other"] += 1

        total = sum(regions.values()) or 1
        result = []
        for region, count in sorted(regions.items(), key=lambda x: -x[1]):
            if count > 0:
                result.append({
                    "region": region,
                    "count": count,
                    "percentage": round(count / total * 100, 1),
                })

        return {"regions": result, "total_incidents": total}

    # ── Confidence Distribution (from DB incidents) ─────────────────

    def get_confidence_distribution(self) -> Dict[str, Any]:
        """
        Histogram of confidence_score for incidents, binned at 0.1 intervals.
        Separate from the log-based histogram in pipeline.py.
        """
        incidents = self.db.query(Incident).filter(
            Incident.confidence_score.isnot(None)
        ).all()

        bins = {
            "0.0-0.2": 0, "0.2-0.4": 0, "0.4-0.6": 0,
            "0.6-0.8": 0, "0.8-1.0": 0,
        }

        for inc in incidents:
            score = inc.confidence_score
            if score is None:
                continue
            if score < 0.2:
                bins["0.0-0.2"] += 1
            elif score < 0.4:
                bins["0.2-0.4"] += 1
            elif score < 0.6:
                bins["0.4-0.6"] += 1
            elif score < 0.8:
                bins["0.6-0.8"] += 1
            else:
                bins["0.8-1.0"] += 1

        return {
            "labels": list(bins.keys()),
            "counts": list(bins.values()),
            "total": sum(bins.values()),
        }

    # ── Detection Latency ───────────────────────────────────────────

    def get_detection_latency(self) -> Dict[str, Any]:
        """
        Calculates time delta between the anomaly event timestamp
        (stored in extra_metadata) and the incident detection_time.
        Returns avg, min, max latency in seconds.
        """
        latencies: List[float] = []
        incidents = self.db.query(Incident).filter(
            Incident.extra_metadata.isnot(None),
            Incident.detection_time.isnot(None),
        ).all()

        for inc in incidents:
            meta = inc.extra_metadata
            if isinstance(meta, str):
                try:
                    meta = json.loads(meta)
                except (json.JSONDecodeError, TypeError):
                    continue
            if not isinstance(meta, dict):
                continue

            event_ts_str = meta.get("timestamp") or meta.get("created_at")
            if not event_ts_str:
                continue

            event_ts = _parse_iso_datetime(str(event_ts_str))
            if event_ts is None:
                continue

            det_time = inc.detection_time
            if isinstance(det_time, str):
                det_time = _parse_iso_datetime(det_time)
            elif isinstance(det_time, datetime):
                pass
            else:
                continue

            if det_time and event_ts:
                det_utc = _to_utc_aware(det_time)
                evt_utc = _to_utc_aware(event_ts)
                delta = abs((det_utc - evt_utc).total_seconds())
                latencies.append(delta)

        if not latencies:
            return {"avg_seconds": 0, "min_seconds": 0, "max_seconds": 0, "sample_count": 0}

        return {
            "avg_seconds": round(sum(latencies) / len(latencies), 1),
            "min_seconds": round(min(latencies), 1),
            "max_seconds": round(max(latencies), 1),
            "sample_count": len(latencies),
        }

    # ── Operational KPIs ────────────────────────────────────────────

    def get_operational_kpis(self) -> Dict[str, Any]:
        """Consolidated operational KPIs for the analytics dashboard."""
        total = self.db.query(func.count(Incident.id)).scalar() or 0
        active = self.db.query(func.count(Incident.id)).filter(
            Incident.status.in_(["detected", "confirmed"])
        ).scalar() or 0
        confirmed = self.db.query(func.count(Incident.id)).filter(
            Incident.status == "confirmed"
        ).scalar() or 0
        resolved = self.db.query(func.count(Incident.id)).filter(
            Incident.status == "resolved"
        ).scalar() or 0
        false_pos = self.db.query(func.count(Incident.id)).filter(
            Incident.status == "false_positive"
        ).scalar() or 0
        fp_rate = round(false_pos / total * 100, 1) if total > 0 else 0.0

        avg_proc = self.db.query(func.avg(Incident.processing_time)).filter(
            Incident.processing_time.isnot(None)
        ).scalar() or 0.0

        avg_conf = self.db.query(func.avg(Incident.confidence_score)).filter(
            Incident.confidence_score.isnot(None)
        ).scalar() or 0.0

        # Total predictions
        total_predictions = self.db.query(func.count(Prediction.id)).scalar() or 0
        oil_spill_predictions = self.db.query(func.count(Prediction.id)).filter(
            Prediction.prediction == "oil_spill"
        ).scalar() or 0

        # Detection latency (quick summary)
        latency = self.get_detection_latency()

        return {
            "total_incidents": total,
            "active_incidents": active,
            "confirmed_spills": confirmed,
            "resolved_incidents": resolved,
            "false_positives": false_pos,
            "false_positive_rate": fp_rate,
            "avg_processing_time": round(avg_proc, 2),
            "avg_confidence": round(avg_conf, 3),
            "total_predictions": total_predictions,
            "oil_spill_detections": oil_spill_predictions,
            "avg_detection_latency_sec": latency["avg_seconds"],
        }

    # ── Incident Lifecycle ──────────────────────────────────────────

    def get_incident_lifecycle(self) -> Dict[str, Any]:
        """
        Status flow summary: how many incidents are in each stage
        and the transition counts.
        """
        incidents = self.db.query(Incident).all()

        status_counts: Dict[str, int] = Counter()
        for inc in incidents:
            status_counts[inc.status or "unknown"] += 1

        # Define the canonical lifecycle stages
        lifecycle_stages = [
            "detected", "confirmed", "resolved", "false_positive", "failed"
        ]
        
        # Intermediate pipeline states mapping (internal -> user-facing)
        internal_to_user = {
            "pending_imagery": "detected",
            "imagery_available": "detected",
            "downloading": "detected",
            "processing": "detected",
            "VERIFIED": "detected",  # Legacy support
            "CONFIRMED": "confirmed",
            "RESOLVED": "resolved",
            "FALSE_POSITIVE": "false_positive",
            "FAILED": "failed",
            "DETECTED": "detected",
        }

        normalized: Dict[str, int] = defaultdict(int)
        for status, count in status_counts.items():
            key = internal_to_user.get(status, status.lower())
            normalized[key] += count

        stages = []
        for stage in lifecycle_stages:
            stages.append({
                "stage": stage,
                "count": normalized.get(stage, 0),
                "label": stage.replace("_", " ").title(),
            })

        return {
            "stages": stages,
            "total": sum(s["count"] for s in stages),
            "raw_status_counts": dict(status_counts),
        }
