"""
Analytics router — operational intelligence endpoints.

All data comes from existing Incident/Prediction models. No new tables.
"""

from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session
from app.database import get_db
from app.services.analytics_service import AnalyticsService
from app.cache import cache_get, cache_set

router = APIRouter()


@router.get("/trends")
async def get_detection_trends(
    period: str = Query("weekly", regex="^(weekly|monthly)$"),
    weeks: int = Query(12, ge=1, le=52),
    db: Session = Depends(get_db),
):
    """Weekly or monthly detection trend counts."""
    key = f"analytics:trends:{period}:{weeks}"
    cached = cache_get(key)
    if cached:
        return cached
    service = AnalyticsService(db)
    result = service.get_detection_trends(period=period, weeks=weeks)
    cache_set(key, result, ttl=120)
    return result


@router.get("/peak-hours")
async def get_peak_hours(db: Session = Depends(get_db)):
    """Hour-of-day detection distribution."""
    cached = cache_get("analytics:peak-hours")
    if cached:
        return cached
    service = AnalyticsService(db)
    result = service.get_peak_hours()
    cache_set("analytics:peak-hours", result, ttl=300)
    return result


@router.get("/regional-density")
async def get_regional_density(db: Session = Depends(get_db)):
    """Spill density by ocean region."""
    cached = cache_get("analytics:regional-density")
    if cached:
        return cached
    service = AnalyticsService(db)
    result = service.get_regional_density()
    cache_set("analytics:regional-density", result, ttl=300)
    return result


@router.get("/confidence-distribution")
async def get_confidence_distribution(db: Session = Depends(get_db)):
    """Confidence score histogram from DB incidents."""
    cached = cache_get("analytics:confidence-dist")
    if cached:
        return cached
    service = AnalyticsService(db)
    result = service.get_confidence_distribution()
    cache_set("analytics:confidence-dist", result, ttl=120)
    return result


@router.get("/detection-latency")
async def get_detection_latency(db: Session = Depends(get_db)):
    """Latency statistics between anomaly event and incident creation."""
    cached = cache_get("analytics:detection-latency")
    if cached:
        return cached
    service = AnalyticsService(db)
    result = service.get_detection_latency()
    cache_set("analytics:detection-latency", result, ttl=120)
    return result


@router.get("/operational-kpis")
async def get_operational_kpis(db: Session = Depends(get_db)):
    """Consolidated operational KPIs."""
    cached = cache_get("analytics:kpis")
    if cached:
        return cached
    service = AnalyticsService(db)
    result = service.get_operational_kpis()
    cache_set("analytics:kpis", result, ttl=60)
    return result


@router.get("/incident-lifecycle")
async def get_incident_lifecycle(db: Session = Depends(get_db)):
    """Incident status lifecycle flow data."""
    cached = cache_get("analytics:lifecycle")
    if cached:
        return cached
    service = AnalyticsService(db)
    result = service.get_incident_lifecycle()
    cache_set("analytics:lifecycle", result, ttl=120)
    return result
