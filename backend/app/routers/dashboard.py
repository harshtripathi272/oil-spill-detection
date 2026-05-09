from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from typing import List
from app.database import get_db
from app.services.dashboard_service import DashboardService
from app.schemas.dashboard import (
    DashboardResponse, DashboardStats, ChartData,
    Incident, DagRun, Metric
)
from app.cache import cache_get, cache_set

router = APIRouter()

@router.get("/stats", response_model=DashboardStats)
async def get_dashboard_stats(db: Session = Depends(get_db)):
    """Get overall dashboard statistics — cached 30 s"""
    cached = cache_get("dashboard:stats")
    if cached:
        return DashboardStats(**cached)
    service = DashboardService(db)
    result = service.get_dashboard_stats()
    cache_set("dashboard:stats", result.model_dump(), ttl=30)
    return result

@router.get("/overview", response_model=DashboardResponse)
async def get_dashboard_overview(db: Session = Depends(get_db)):
    """Get complete dashboard overview — cached 60 s"""
    cached = cache_get("dashboard:overview")
    if cached:
        return cached
    service = DashboardService(db)
    stats = service.get_dashboard_stats()
    recent_incidents = service.get_recent_incidents()
    processing_times = service.get_processing_times_chart()
    incidents_over_time = service.get_incidents_over_time()
    status_distribution = service.get_status_distribution()
    model_performance = service.get_model_performance()
    response = DashboardResponse(
        stats=stats,
        recent_incidents=recent_incidents,
        processing_times_chart=processing_times,
        incidents_over_time=incidents_over_time,
        status_distribution=status_distribution,
        model_performance=model_performance
    )
    cache_set("dashboard:overview", response.model_dump(), ttl=60)
    return response

@router.get("/charts/incidents-over-time", response_model=ChartData)
async def get_incidents_over_time(days: int = 30, db: Session = Depends(get_db)):
    """Get incidents over time chart data — cached 5 min"""
    key = f"dashboard:chart:incidents-over-time:{days}"
    cached = cache_get(key)
    if cached:
        return cached
    service = DashboardService(db)
    result = service.get_incidents_over_time(days)
    cache_set(key, result.model_dump(), ttl=300)
    return result

@router.get("/charts/processing-times", response_model=ChartData)
async def get_processing_times_chart(days: int = 30, db: Session = Depends(get_db)):
    """Get processing times chart data"""
    service = DashboardService(db)
    return service.get_processing_times_chart(days)

@router.get("/charts/status-distribution", response_model=ChartData)
async def get_status_distribution(db: Session = Depends(get_db)):
    """Get incident status distribution chart data"""
    service = DashboardService(db)
    return service.get_status_distribution()

@router.get("/charts/model-performance", response_model=ChartData)
async def get_model_performance(days: int = 30, db: Session = Depends(get_db)):
    """Get model performance chart data"""
    service = DashboardService(db)
    return service.get_model_performance(days)

@router.get("/charts/dag-run-performance", response_model=ChartData)
async def get_dag_run_performance(days: int = 7, db: Session = Depends(get_db)):
    """Get DAG run performance chart data"""
    service = DashboardService(db)
    return service.get_dag_run_performance(days)

@router.get("/recent-incidents", response_model=List[Incident])
async def get_recent_incidents(limit: int = 10, db: Session = Depends(get_db)):
    """Get recent incidents"""
    service = DashboardService(db)
    return service.get_recent_incidents(limit)

@router.get("/metrics/summary")
async def get_metrics_summary(db: Session = Depends(get_db)):
    """Get summary of key metrics"""
    service = DashboardService(db)
    stats = service.get_dashboard_stats()

    return {
        "total_incidents_today": stats.total_incidents,
        "active_incidents": stats.active_incidents,
        "success_rate": round((stats.successful_runs / stats.total_dag_runs * 100), 2) if stats.total_dag_runs > 0 else 0,
        "avg_processing_time": stats.avg_processing_time,
        "avg_confidence": stats.avg_confidence_score
    }