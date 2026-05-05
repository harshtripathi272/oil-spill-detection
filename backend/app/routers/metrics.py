from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from typing import List, Optional
from datetime import datetime, timedelta
from app.database import get_db
from app.models.incident import Metric
from app.schemas.dashboard import Metric as MetricSchema

router = APIRouter()

@router.get("/", response_model=List[MetricSchema])
async def get_metrics(
    category: Optional[str] = None,
    name: Optional[str] = None,
    limit: int = 100,
    db: Session = Depends(get_db)
):
    """Get metrics with optional filtering"""
    query = db.query(Metric)

    if category:
        query = query.filter(Metric.category == category)

    if name:
        query = query.filter(Metric.name == name)

    metrics = query.order_by(Metric.timestamp.desc()).limit(limit).all()
    return metrics

@router.get("/categories")
async def get_metric_categories(db: Session = Depends(get_db)):
    """Get available metric categories"""
    from sqlalchemy import distinct

    categories = db.query(distinct(Metric.category)).all()
    return [cat[0] for cat in categories]

@router.get("/names")
async def get_metric_names(db: Session = Depends(get_db)):
    """Get available metric names"""
    from sqlalchemy import distinct

    names = db.query(distinct(Metric.name)).all()
    return [name[0] for name in names]

@router.get("/time-series/{metric_name}")
async def get_metric_time_series(
    metric_name: str,
    hours: int = 24,
    db: Session = Depends(get_db)
):
    """Get time series data for a specific metric"""
    start_time = datetime.utcnow() - timedelta(hours=hours)

    metrics = db.query(Metric).filter(
        Metric.name == metric_name,
        Metric.timestamp >= start_time
    ).order_by(Metric.timestamp).all()

    time_series = {
        "timestamps": [m.timestamp.isoformat() for m in metrics],
        "values": [m.value for m in metrics],
        "metric_name": metric_name
    }

    return time_series

@router.get("/system/health")
async def get_system_health_metrics(db: Session = Depends(get_db)):
    """Get system health metrics"""
    # Get recent system metrics
    recent_metrics = db.query(Metric).filter(
        Metric.category == "system",
        Metric.timestamp >= datetime.utcnow() - timedelta(hours=1)
    ).all()

    health_data = {}
    for metric in recent_metrics:
        health_data[metric.name] = {
            "value": metric.value,
            "timestamp": metric.timestamp.isoformat(),
            "metadata": metric.metadata
        }

    return health_data

@router.get("/model/performance")
async def get_model_performance_metrics(db: Session = Depends(get_db)):
    """Get model performance metrics"""
    # Get recent model metrics
    recent_metrics = db.query(Metric).filter(
        Metric.category == "model",
        Metric.timestamp >= datetime.utcnow() - timedelta(days=7)
    ).order_by(Metric.timestamp.desc()).all()

    performance_data = {}
    for metric in recent_metrics:
        if metric.name not in performance_data:
            performance_data[metric.name] = []
        performance_data[metric.name].append({
            "value": metric.value,
            "timestamp": metric.timestamp.isoformat(),
            "metadata": metric.metadata
        })

    return performance_data

@router.get("/processing/stats")
async def get_processing_stats(db: Session = Depends(get_db)):
    """Get processing statistics"""
    from sqlalchemy import func

    # Get processing metrics from last 24 hours
    start_time = datetime.utcnow() - timedelta(hours=24)

    stats = db.query(
        func.avg(Metric.value).label('avg_value'),
        func.min(Metric.value).label('min_value'),
        func.max(Metric.value).label('max_value'),
        func.count(Metric.id).label('count')
    ).filter(
        Metric.category == "processing",
        Metric.timestamp >= start_time
    ).first()

    return {
        "average_processing_time": round(stats.avg_value or 0, 2),
        "min_processing_time": round(stats.min_value or 0, 2),
        "max_processing_time": round(stats.max_value or 0, 2),
        "total_processed": stats.count
    }

@router.post("/")
async def create_metric(
    name: str,
    value: float,
    category: str,
    metadata: Optional[dict] = None,
    db: Session = Depends(get_db)
):
    """Create a new metric"""
    metric = Metric(
        name=name,
        value=value,
        category=category,
        metadata=metadata
    )

    db.add(metric)
    db.commit()
    db.refresh(metric)

    return {"message": "Metric created successfully", "id": metric.id}

@router.get("/summary")
async def get_metrics_summary(db: Session = Depends(get_db)):
    """Get summary of all metric categories"""
    from sqlalchemy import func

    # Get latest metrics for each category
    categories = ["system", "model", "processing", "airflow"]

    summary = {}
    for category in categories:
        latest_metric = db.query(Metric).filter(
            Metric.category == category
        ).order_by(Metric.timestamp.desc()).first()

        if latest_metric:
            summary[category] = {
                "latest_value": latest_metric.value,
                "latest_timestamp": latest_metric.timestamp.isoformat(),
                "metric_name": latest_metric.name
            }
        else:
            summary[category] = None

    return summary