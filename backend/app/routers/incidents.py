from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from typing import List, Optional
from datetime import datetime, timedelta
from app.database import get_db, supabase
from app.models.incident import Incident, DagRun
from app.schemas.dashboard import Incident as IncidentSchema, DagRun as DagRunSchema

router = APIRouter()

@router.get("/", response_model=List[IncidentSchema])
async def get_incidents(
    skip: int = 0,
    limit: int = 100,
    status: Optional[str] = None,
    min_confidence: Optional[float] = None,
    db: Session = Depends(get_db)
):
    """Get incidents with optional filtering"""
    if supabase.is_configured:
        incidents = supabase.list_incidents(
            status=status,
            min_confidence=min_confidence,
            limit=limit,
            offset=skip,
        )
        return [IncidentSchema(**incident) for incident in incidents]

    query = db.query(Incident)
    if status:
        query = query.filter(Incident.status == status)
    if min_confidence is not None:
        query = query.filter(Incident.confidence_score >= min_confidence)
    incidents = query.offset(skip).limit(limit).all()
    return incidents

@router.get("/{incident_id}", response_model=IncidentSchema)
async def get_incident(incident_id: str, db: Session = Depends(get_db)):
    """Get a specific incident by ID"""
    if supabase.is_configured:
        incident = supabase.get_incident_by_id(incident_id)
        if not incident:
            raise HTTPException(status_code=404, detail="Incident not found")
        return IncidentSchema(**incident)

    incident = db.query(Incident).filter(Incident.id == incident_id).first()
    if not incident:
        raise HTTPException(status_code=404, detail="Incident not found")
    return incident

@router.get("/{incident_id}/dag-runs", response_model=List[DagRunSchema])
async def get_incident_dag_runs(incident_id: str, db: Session = Depends(get_db)):
    """Get DAG runs for a specific incident"""
    if supabase.is_configured:
        rows = supabase.select(
            "dag_runs",
            select="*",
            filters={"incident_id": f"eq.{incident_id}"},
            limit=100,
        )
        return [DagRunSchema(**row) for row in rows]

    incident = db.query(Incident).filter(Incident.id == incident_id).first()
    if not incident:
        raise HTTPException(status_code=404, detail="Incident not found")

    dag_runs = db.query(DagRun).filter(DagRun.incident_id == incident_id).all()
    return dag_runs

@router.put("/{incident_id}/status")
async def update_incident_status(
    incident_id: str,
    status: str,
    db: Session = Depends(get_db)
):
    """Update incident status"""
    valid_statuses = ["DETECTED", "PENDING_IMAGERY", "IMAGERY_AVAILABLE", "DOWNLOADING", "PROCESSING", "VERIFIED", "FALSE_POSITIVE", "FAILED", "detected", "confirmed", "false_positive", "resolved"]
    if status not in valid_statuses:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid status. Must be one of: {', '.join(valid_statuses)}"
        )

    if supabase.is_configured:
        incident = supabase.get_incident_by_id(incident_id)
        if not incident:
            raise HTTPException(status_code=404, detail="Incident not found")
        supabase.upsert_incident(incident_id=incident_id, state=status.upper(), metadata=incident.get("metadata", {}))
        return {"message": f"Incident {incident_id} status updated to {status}"}

    incident = db.query(Incident).filter(Incident.id == incident_id).first()
    if not incident:
        raise HTTPException(status_code=404, detail="Incident not found")

    incident.status = status
    db.commit()
    db.refresh(incident)
    return {"message": f"Incident {incident_id} status updated to {status}"}

@router.get("/stats/status-breakdown")
async def get_incident_status_breakdown(db: Session = Depends(get_db)):
    """Get breakdown of incidents by status"""
    if supabase.is_configured:
        rows = supabase.select(
            "incidents",
            select="state",
            filters={},
            limit=10000,
        )
        breakdown = {}
        for row in rows:
            state = row.get("state")
            if state is None:
                continue
            breakdown[state] = breakdown.get(state, 0) + 1
        return breakdown

    from sqlalchemy import func

    results = db.query(
        Incident.status,
        func.count(Incident.id).label('count')
    ).group_by(Incident.status).all()

    return {result.status: result.count for result in results}

@router.get("/stats/geographic-distribution")
async def get_geographic_distribution(db: Session = Depends(get_db)):
    """Get geographic distribution of incidents"""
    if supabase.is_configured:
        rows = supabase.select("incidents", select="*")
        incidents = [supabase.normalize_incident(row) for row in rows]
    else:
        incidents = db.query(Incident).all()

    regions = {
        "North Atlantic": 0,
        "South Atlantic": 0,
        "Pacific": 0,
        "Indian Ocean": 0,
        "Other": 0
    }

    for incident in incidents:
        lat = getattr(incident, "latitude", None) or incident.get("latitude")
        lon = getattr(incident, "longitude", None) or incident.get("longitude")
        if lat is None or lon is None:
            continue
        if -90 <= lat <= 90 and -180 <= lon <= 180:
            if -30 <= lon <= 30:
                if lat >= 0:
                    regions["North Atlantic"] += 1
                else:
                    regions["South Atlantic"] += 1
            elif 30 <= lon <= 180:
                regions["Pacific"] += 1
            elif -180 <= lon <= -30:
                regions["Pacific"] += 1
            elif 30 <= lon <= 120:
                regions["Indian Ocean"] += 1
            else:
                regions["Other"] += 1

    return regions

@router.get("/timeline")
async def get_incidents_timeline(
    days: int = 30,
    db: Session = Depends(get_db)
):
    """Get incidents timeline data"""
    start_date = datetime.utcnow() - timedelta(days=days)
    if supabase.is_configured:
        rows = supabase.select("incidents", select="*")
        incidents = [supabase.normalize_incident(row) for row in rows]
    else:
        incidents = db.query(Incident).filter(
            Incident.detection_time >= start_date
        ).order_by(Incident.detection_time).all()

    timeline_data = []
    for incident in incidents:
        detection_time = getattr(incident, "detection_time", None) or incident.get("created_at")
        timestamp = detection_time.isoformat() if hasattr(detection_time, "isoformat") else detection_time

        timeline_data.append({
            "id": getattr(incident, "id", None) or incident.get("id"),
            "timestamp": timestamp,
            "latitude": getattr(incident, "latitude", None) or incident.get("latitude"),
            "longitude": getattr(incident, "longitude", None) or incident.get("longitude"),
            "status": getattr(incident, "status", None) or incident.get("status"),
            "confidence": getattr(incident, "confidence_score", None) or incident.get("confidence_score")
        })

    return {"incidents": timeline_data}
