"""
Vessel Intelligence Service.

Extracts vessel data from the extra_metadata JSON field of the incidents
table. Computes explainable risk scores, behavioral profiles, and
chronological timelines — all without new database tables.
"""

import json
import math
from collections import defaultdict
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from sqlalchemy import desc
from sqlalchemy.orm import Session

from app.models.incident import Incident


def _parse_meta(incident: Incident) -> Optional[dict]:
    """Safely parse extra_metadata from an Incident row."""
    meta = incident.extra_metadata
    if meta is None:
        return None
    if isinstance(meta, str):
        try:
            meta = json.loads(meta)
        except (json.JSONDecodeError, TypeError):
            return None
    return meta if isinstance(meta, dict) else None


def _parse_dt(value) -> Optional[datetime]:
    """Parse datetime from string or datetime."""
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        try:
            return datetime.fromisoformat(value.rstrip("Z"))
        except ValueError:
            return None
    return None


class VesselService:
    def __init__(self, db: Session):
        self.db = db
        self._vessel_cache: Optional[Dict[str, List[dict]]] = None

    # ── Internal: build vessel index ────────────────────────────────

    def _build_vessel_index(self) -> Dict[str, List[dict]]:
        """
        Scan all incidents with extra_metadata, group by vessel_id.
        Returns { vessel_id: [list of enriched incident dicts] }.
        """
        if self._vessel_cache is not None:
            return self._vessel_cache

        incidents = self.db.query(Incident).filter(
            Incident.extra_metadata.isnot(None)
        ).order_by(Incident.detection_time).all()

        index: Dict[str, List[dict]] = defaultdict(list)

        for inc in incidents:
            meta = _parse_meta(inc)
            if not meta:
                continue

            vessel_id = meta.get("vessel_id")
            if not vessel_id:
                continue

            # Extract AIS features from nested structure
            raw_block = meta.get("raw", {})
            features = raw_block.get("features", {}) if isinstance(raw_block, dict) else {}
            if not features and isinstance(meta.get("raw"), dict):
                inner_raw = meta["raw"].get("raw", {})
                if isinstance(inner_raw, dict):
                    features = inner_raw.get("features", {})

            # Model scores
            model_block = meta.get("model", {})
            model_meta = model_block.get("metadata", {}) if isinstance(model_block, dict) else {}

            # Inference results
            inference = meta.get("inference", [])
            top_inference = inference[0] if isinstance(inference, list) and inference else {}

            dt = _parse_dt(inc.detection_time)

            entry = {
                "incident_id": inc.id,
                "vessel_id": str(vessel_id),
                "lat": inc.latitude or meta.get("lat"),
                "lon": inc.longitude or meta.get("lon"),
                "timestamp": dt.isoformat() if dt else None,
                "status": inc.status,
                "confidence_score": inc.confidence_score or meta.get("score"),
                "anomaly_score": meta.get("score"),
                # AIS features
                "speed_knots": features.get("speed_knots"),
                "heading_deg": features.get("heading_deg"),
                "cog_deg": features.get("cog_deg"),
                "turn_rate": features.get("turn_rate_deg_per_sec"),
                "acceleration": features.get("acceleration_knots_per_sec"),
                "time_gap_sec": features.get("time_gap_sec"),
                "vessel_type": features.get("vessel_type") or model_meta.get("vessel_type"),
                # Model scores
                "global_score": model_meta.get("global_score"),
                "local_score": model_meta.get("local_score"),
                "vessel_score": model_meta.get("vessel_score"),
                "physics_score": model_meta.get("physics_score"),
                # Inference
                "prediction": top_inference.get("prediction"),
                "prediction_confidence": top_inference.get("confidence"),
                "sar_image": inc.sar_image_path,
                "processed_image": inc.processed_image_path,
            }
            index[str(vessel_id)].append(entry)

        self._vessel_cache = dict(index)
        return self._vessel_cache

    # ── Risk Score Calculation ──────────────────────────────────────

    def _compute_risk_score(self, events: List[dict]) -> Dict[str, Any]:
        """
        Compute an explainable risk score for a vessel.
        Returns composite score (0-100) and per-factor breakdown.
        """
        n = len(events)
        if n == 0:
            return {"score": 0, "factors": {}}

        # Factor 1: Proximity incidents (more incidents = higher risk)
        proximity_factor = min(n / 10.0, 1.0)  # caps at 10 incidents

        # Factor 2: Average anomaly score
        anomaly_scores = [e["anomaly_score"] for e in events if e.get("anomaly_score")]
        avg_anomaly = sum(anomaly_scores) / len(anomaly_scores) if anomaly_scores else 0.0

        # Factor 3: Speed anomalies (speed ≤ 0.5 knots in open water = suspicious)
        speed_vals = [e["speed_knots"] for e in events if e.get("speed_knots") is not None]
        speed_anomalies = sum(1 for s in speed_vals if s <= 0.5) if speed_vals else 0
        speed_factor = min(speed_anomalies / 3.0, 1.0) if speed_vals else 0.0

        # Factor 4: Heading anomalies (high turn rates)
        turn_rates = [abs(e["turn_rate"]) for e in events if e.get("turn_rate") is not None]
        heading_anomalies = sum(1 for t in turn_rates if t > 1.0) if turn_rates else 0
        heading_factor = min(heading_anomalies / 3.0, 1.0) if turn_rates else 0.0

        # Factor 5: Oil spill confirmations
        oil_spills = sum(1 for e in events if e.get("prediction") == "oil_spill")
        spill_factor = min(oil_spills / 5.0, 1.0)

        # Weighted composite score (0-100)
        weights = {
            "proximity": 0.20,
            "anomaly_score": 0.25,
            "speed_anomalies": 0.15,
            "heading_anomalies": 0.10,
            "oil_spill_association": 0.30,
        }

        composite = (
            weights["proximity"] * proximity_factor +
            weights["anomaly_score"] * avg_anomaly +
            weights["speed_anomalies"] * speed_factor +
            weights["heading_anomalies"] * heading_factor +
            weights["oil_spill_association"] * spill_factor
        )
        score = round(min(composite * 100, 100), 1)

        return {
            "score": score,
            "factors": {
                "proximity_incidents": {
                    "value": n,
                    "weight": weights["proximity"],
                    "contribution": round(proximity_factor * weights["proximity"] * 100, 1),
                    "description": f"{n} associated incidents",
                },
                "avg_anomaly_score": {
                    "value": round(avg_anomaly, 3),
                    "weight": weights["anomaly_score"],
                    "contribution": round(avg_anomaly * weights["anomaly_score"] * 100, 1),
                    "description": f"Average model anomaly score: {round(avg_anomaly, 3)}",
                },
                "speed_anomalies": {
                    "value": speed_anomalies,
                    "weight": weights["speed_anomalies"],
                    "contribution": round(speed_factor * weights["speed_anomalies"] * 100, 1),
                    "description": f"{speed_anomalies} low-speed events (≤0.5 kn)",
                },
                "heading_anomalies": {
                    "value": heading_anomalies,
                    "weight": weights["heading_anomalies"],
                    "contribution": round(heading_factor * weights["heading_anomalies"] * 100, 1),
                    "description": f"{heading_anomalies} high turn-rate events",
                },
                "oil_spill_association": {
                    "value": oil_spills,
                    "weight": weights["oil_spill_association"],
                    "contribution": round(spill_factor * weights["oil_spill_association"] * 100, 1),
                    "description": f"{oil_spills} confirmed oil spill detections",
                },
            },
        }

    # ── Public API Methods ──────────────────────────────────────────

    def list_vessels(
        self,
        sort_by: str = "risk_score",
        limit: int = 50,
        offset: int = 0,
    ) -> Dict[str, Any]:
        """List all vessels with risk scores, paginated."""
        index = self._build_vessel_index()

        vessel_list = []
        for vessel_id, events in index.items():
            risk = self._compute_risk_score(events)
            latest = events[-1]  # last event chronologically

            vessel_list.append({
                "vessel_id": vessel_id,
                "vessel_type": latest.get("vessel_type") or "Unknown",
                "incident_count": len(events),
                "risk_score": risk["score"],
                "avg_anomaly_score": round(
                    sum(e["anomaly_score"] for e in events if e.get("anomaly_score")) /
                    max(1, sum(1 for e in events if e.get("anomaly_score"))),
                    3,
                ),
                "oil_spill_count": sum(1 for e in events if e.get("prediction") == "oil_spill"),
                "last_seen": latest.get("timestamp"),
                "last_lat": latest.get("lat"),
                "last_lon": latest.get("lon"),
                "last_status": latest.get("status"),
            })

        # Sort
        if sort_by == "risk_score":
            vessel_list.sort(key=lambda v: v["risk_score"], reverse=True)
        elif sort_by == "incident_count":
            vessel_list.sort(key=lambda v: v["incident_count"], reverse=True)
        elif sort_by == "last_seen":
            vessel_list.sort(key=lambda v: v["last_seen"] or "", reverse=True)

        total = len(vessel_list)
        page = vessel_list[offset:offset + limit]

        return {"vessels": page, "total": total}

    def get_vessel_detail(self, vessel_id: str) -> Optional[Dict[str, Any]]:
        """Detailed vessel profile with risk factor breakdown."""
        index = self._build_vessel_index()
        events = index.get(vessel_id)
        if not events:
            return None

        risk = self._compute_risk_score(events)
        latest = events[-1]

        return {
            "vessel_id": vessel_id,
            "vessel_type": latest.get("vessel_type") or "Unknown",
            "incident_count": len(events),
            "oil_spill_count": sum(1 for e in events if e.get("prediction") == "oil_spill"),
            "last_seen": latest.get("timestamp"),
            "last_lat": latest.get("lat"),
            "last_lon": latest.get("lon"),
            "risk": risk,
            "incidents": [
                {
                    "incident_id": e["incident_id"],
                    "timestamp": e["timestamp"],
                    "lat": e["lat"],
                    "lon": e["lon"],
                    "status": e["status"],
                    "anomaly_score": e.get("anomaly_score"),
                    "prediction": e.get("prediction"),
                    "prediction_confidence": e.get("prediction_confidence"),
                    "sar_image": e.get("sar_image"),
                    "processed_image": e.get("processed_image"),
                }
                for e in events
            ],
        }

    def get_vessel_timeline(self, vessel_id: str) -> Optional[List[dict]]:
        """Chronological event timeline for a specific vessel."""
        index = self._build_vessel_index()
        events = index.get(vessel_id)
        if not events:
            return None

        return [
            {
                "incident_id": e["incident_id"],
                "timestamp": e["timestamp"],
                "lat": e["lat"],
                "lon": e["lon"],
                "status": e["status"],
                "anomaly_score": e.get("anomaly_score"),
                "prediction": e.get("prediction"),
                "speed_knots": e.get("speed_knots"),
                "heading_deg": e.get("heading_deg"),
            }
            for e in events
        ]

    def get_vessel_behavior(self, vessel_id: str) -> Optional[Dict[str, Any]]:
        """Speed and heading time-series for behavioral analysis."""
        index = self._build_vessel_index()
        events = index.get(vessel_id)
        if not events:
            return None

        timestamps = []
        speeds = []
        headings = []
        turn_rates = []

        for e in events:
            timestamps.append(e.get("timestamp"))
            speeds.append(e.get("speed_knots"))
            headings.append(e.get("heading_deg"))
            turn_rates.append(e.get("turn_rate"))

        # Calculate averages for summary
        valid_speeds = [s for s in speeds if s is not None]
        valid_headings = [h for h in headings if h is not None]

        return {
            "vessel_id": vessel_id,
            "timestamps": timestamps,
            "speeds": speeds,
            "headings": headings,
            "turn_rates": turn_rates,
            "summary": {
                "avg_speed": round(sum(valid_speeds) / len(valid_speeds), 2) if valid_speeds else None,
                "max_speed": round(max(valid_speeds), 2) if valid_speeds else None,
                "min_speed": round(min(valid_speeds), 2) if valid_speeds else None,
                "speed_variance": round(
                    sum((s - sum(valid_speeds) / len(valid_speeds)) ** 2 for s in valid_speeds) / len(valid_speeds),
                    3,
                ) if len(valid_speeds) > 1 else 0,
                "avg_heading": round(sum(valid_headings) / len(valid_headings), 1) if valid_headings else None,
                "data_points": len(events),
            },
        }

    def get_watchlist(self, limit: int = 10) -> List[dict]:
        """Top high-risk vessels for the watchlist."""
        result = self.list_vessels(sort_by="risk_score", limit=limit, offset=0)
        return result["vessels"]
