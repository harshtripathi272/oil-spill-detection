from pydantic import BaseModel, Field
from typing import Optional, List, Dict, Any
from datetime import datetime

# Incident schemas
class IncidentBase(BaseModel):
    id: str
    latitude: float
    longitude: float
    confidence_score: Optional[float] = None
    status: str = "detected"
    bbox_coordinates: Optional[Dict[str, Any]] = None
    model_version: Optional[str] = None
    processing_time: Optional[float] = None

class IncidentCreate(IncidentBase):
    pass

class Incident(IncidentBase):
    detection_time: datetime
    sar_image_path: Optional[str] = None
    processed_image_path: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None

    class Config:
        from_attributes = True

# DAG Run schemas
class DagRunBase(BaseModel):
    dag_id: str
    run_id: str
    state: str
    start_date: Optional[datetime] = None
    end_date: Optional[datetime] = None
    execution_time: Optional[float] = None

class DagRun(DagRunBase):
    id: int
    incident_id: Optional[str] = None

    class Config:
        from_attributes = True

# Metric schemas
class MetricBase(BaseModel):
    name: str
    value: float
    category: str
    metadata: Optional[Dict[str, Any]] = None

class Metric(MetricBase):
    id: int
    timestamp: datetime

    class Config:
        from_attributes = True

# Dashboard schemas
class DashboardStats(BaseModel):
    total_incidents: int
    active_incidents: int
    resolved_incidents: int
    false_positives: int
    avg_processing_time: float
    total_dag_runs: int
    successful_runs: int
    failed_runs: int
    avg_confidence_score: float

class TimeSeriesData(BaseModel):
    timestamps: List[datetime]
    values: List[float]
    labels: List[str]

class ChartData(BaseModel):
    title: str
    data: Dict[str, Any]
    chart_type: str  # line, bar, pie, etc.

class DashboardResponse(BaseModel):
    stats: DashboardStats
    recent_incidents: List[Incident]
    processing_times_chart: ChartData
    incidents_over_time: ChartData
    status_distribution: ChartData
    model_performance: ChartData

# System schemas
class SystemStatus(BaseModel):
    component: str
    status: str
    last_check: datetime
    details: Optional[Dict[str, Any]] = None

class SystemHealth(BaseModel):
    overall_status: str
    components: List[SystemStatus]
    uptime: float
    last_updated: datetime