from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from typing import List
from app.database import get_db
from app.models.alerts import Alert
from app.models.incident import Incident
from app.schemas.dashboard import Alert as AlertSchema
from sqlalchemy import desc

router = APIRouter()

@router.get("", response_model=List[AlertSchema])
async def list_alerts(limit: int = 20, db: Session = Depends(get_db)):
    alerts = db.query(Alert).order_by(desc(Alert.created_at)).limit(limit).all()
    if not alerts:
        incidents = db.query(Incident).order_by(desc(Incident.detection_time)).limit(limit).all()
        alerts = []
        for incident in incidents:
            level = "critical" if incident.confidence_score and incident.confidence_score > 0.85 else "warning" if incident.confidence_score and incident.confidence_score > 0.65 else "low"
            alerts.append(Alert(
                incident_id=incident.id,
                level=level,
                message=f"Incident {incident.id} detected with confidence {incident.confidence_score or 0:.2f}",
                extra_metadata={
                    "status": incident.status,
                    "latitude": incident.latitude,
                    "longitude": incident.longitude
                }
            ))
        return alerts
    return alerts

@router.post("/{alert_id}/acknowledge")
async def acknowledge_alert(alert_id: int, db: Session = Depends(get_db)):
    alert = db.query(Alert).filter(Alert.id == alert_id).first()
    if not alert:
        raise HTTPException(status_code=404, detail="Alert not found")
    alert.acknowledged = True
    db.commit()
    db.refresh(alert)
    return {"message": "Alert acknowledged", "id": alert.id}

@router.post("")
async def create_alert(
    incident_id: str,
    level: str,
    message: str,
    db: Session = Depends(get_db)
):
    alert = Alert(
        incident_id=incident_id,
        level=level,
        message=message,
        extra_metadata={"created_by": "system"}
    )
    db.add(alert)
    db.commit()
    db.refresh(alert)
    return {"message": "Alert created successfully", "id": alert.id}
