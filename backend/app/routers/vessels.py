"""
Vessel Intelligence router — vessel-centric analytics and risk profiling.

All data is derived from existing Incident.extra_metadata. No new tables.
"""

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from app.database import get_db
from app.services.vessel_service import VesselService
from app.cache import cache_get, cache_set

router = APIRouter()


def _require_vessel_id(vessel_id: str) -> str:
    vid = (vessel_id or "").strip()
    if not vid:
        raise HTTPException(status_code=400, detail="vessel_id is required")
    return vid


@router.get("/")
@router.get("", include_in_schema=False)
async def list_vessels(
    sort_by: str = Query("risk_score", regex="^(risk_score|incident_count|last_seen)$"),
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
    db: Session = Depends(get_db),
):
    """List all tracked vessels with risk scores."""
    key = f"vessels:list:{sort_by}:{limit}:{offset}"
    cached = cache_get(key)
    if cached:
        return cached
    service = VesselService(db)
    result = service.list_vessels(sort_by=sort_by, limit=limit, offset=offset)
    cache_set(key, result, ttl=60)
    return result


@router.get("/watchlist")
async def get_watchlist(
    limit: int = Query(10, ge=1, le=50),
    db: Session = Depends(get_db),
):
    """Top high-risk vessels for the watchlist."""
    cached = cache_get(f"vessels:watchlist:{limit}")
    if cached:
        return cached
    service = VesselService(db)
    result = service.get_watchlist(limit=limit)
    cache_set(f"vessels:watchlist:{limit}", result, ttl=60)
    return result


@router.get("/{vessel_id}")
async def get_vessel_detail(vessel_id: str, db: Session = Depends(get_db)):
    """Detailed vessel profile with risk factor breakdown."""
    vessel_id = _require_vessel_id(vessel_id)
    service = VesselService(db)
    result = service.get_vessel_detail(vessel_id)
    if result is None:
        raise HTTPException(status_code=404, detail=f"Vessel {vessel_id} not found")
    return result


@router.get("/{vessel_id}/timeline")
async def get_vessel_timeline(vessel_id: str, db: Session = Depends(get_db)):
    """Chronological event timeline for a specific vessel."""
    vessel_id = _require_vessel_id(vessel_id)
    service = VesselService(db)
    result = service.get_vessel_timeline(vessel_id)
    if result is None:
        raise HTTPException(status_code=404, detail=f"Vessel {vessel_id} not found")
    return result


@router.get("/{vessel_id}/behavior")
async def get_vessel_behavior(vessel_id: str, db: Session = Depends(get_db)):
    """Speed and heading behavioral time-series data."""
    vessel_id = _require_vessel_id(vessel_id)
    service = VesselService(db)
    result = service.get_vessel_behavior(vessel_id)
    if result is None:
        raise HTTPException(status_code=404, detail=f"Vessel {vessel_id} not found")
    return result
