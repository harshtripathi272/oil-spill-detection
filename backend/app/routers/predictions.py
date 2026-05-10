from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from typing import List, Optional
from datetime import datetime, timedelta
from fastapi.responses import FileResponse
import os

from app.database import get_db
from app.models.predictions import Prediction
from app.schemas.predictions import PredictionCreate, PredictionResponse, PredictionUpdate
from app.placeholders import prediction_image_placeholder

router = APIRouter(tags=["predictions"])

# oil-spill-detection/ (contains sentinel_data/) — from backend/app/routers/ this file
_PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))


def _resolved_prediction_image_path(raw: Optional[str]) -> Optional[str]:
    if not raw or not str(raw).strip():
        return None
    p = str(raw).strip()
    if os.path.isabs(p):
        return p
    return os.path.normpath(os.path.join(_PROJECT_ROOT, p))

@router.get("/", response_model=List[PredictionResponse])
@router.get("", response_model=List[PredictionResponse], include_in_schema=False)
def get_predictions(
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=1000),
    incident_id: Optional[str] = None,
    prediction: Optional[str] = None,
    min_confidence: Optional[float] = Query(None, ge=0.0, le=1.0),
    max_confidence: Optional[float] = Query(None, ge=0.0, le=1.0),
    start_date: Optional[datetime] = None,
    end_date: Optional[datetime] = None,
    db: Session = Depends(get_db)
):
    """Get all predictions with optional filtering"""
    query = db.query(Prediction)

    if incident_id:
        query = query.filter(Prediction.incident_id == incident_id)

    if prediction:
        query = query.filter(Prediction.prediction == prediction)

    if min_confidence is not None:
        query = query.filter(Prediction.confidence >= min_confidence)

    if max_confidence is not None:
        query = query.filter(Prediction.confidence <= max_confidence)

    if start_date:
        query = query.filter(Prediction.created_at >= start_date)

    if end_date:
        query = query.filter(Prediction.created_at <= end_date)

    predictions = query.offset(skip).limit(limit).all()
    return predictions

@router.get("/{prediction_id}", response_model=PredictionResponse)
def get_prediction(prediction_id: int, db: Session = Depends(get_db)):
    """Get a specific prediction by ID"""
    prediction = db.query(Prediction).filter(Prediction.id == prediction_id).first()
    if not prediction:
        raise HTTPException(status_code=404, detail="Prediction not found")
    return prediction

@router.post("/", response_model=PredictionResponse)
def create_prediction(prediction: PredictionCreate, db: Session = Depends(get_db)):
    """Create a new prediction"""
    db_prediction = Prediction(**prediction.dict())
    db.add(db_prediction)
    db.commit()
    db.refresh(db_prediction)
    return db_prediction

@router.put("/{prediction_id}", response_model=PredictionResponse)
def update_prediction(
    prediction_id: int,
    prediction_update: PredictionUpdate,
    db: Session = Depends(get_db)
):
    """Update a prediction"""
    prediction = db.query(Prediction).filter(Prediction.id == prediction_id).first()
    if not prediction:
        raise HTTPException(status_code=404, detail="Prediction not found")

    for field, value in prediction_update.dict(exclude_unset=True).items():
        setattr(prediction, field, value)

    db.commit()
    db.refresh(prediction)
    return prediction

@router.delete("/{prediction_id}")
def delete_prediction(prediction_id: int, db: Session = Depends(get_db)):
    """Delete a prediction"""
    prediction = db.query(Prediction).filter(Prediction.id == prediction_id).first()
    if not prediction:
        raise HTTPException(status_code=404, detail="Prediction not found")

    db.delete(prediction)
    db.commit()
    return {"message": "Prediction deleted successfully"}

@router.get("/stats/summary")
def get_prediction_stats(db: Session = Depends(get_db)):
    """Get prediction statistics"""
    total_predictions = db.query(Prediction).count()

    # Count by prediction type
    oil_spill_count = db.query(Prediction).filter(Prediction.prediction == "oil_spill").count()
    no_oil_spill_count = db.query(Prediction).filter(Prediction.prediction == "no_oil_spill").count()

    # Average confidence
    avg_confidence = db.query(Prediction).filter(Prediction.confidence.isnot(None)).all()
    if avg_confidence:
        avg_confidence = sum(p.confidence for p in avg_confidence) / len(avg_confidence)
    else:
        avg_confidence = 0.0

    # Recent predictions (last 24 hours)
    yesterday = datetime.utcnow() - timedelta(days=1)
    recent_predictions = db.query(Prediction).filter(Prediction.created_at >= yesterday).count()

    return {
        "total_predictions": total_predictions,
        "oil_spill_predictions": oil_spill_count,
        "no_oil_spill_predictions": no_oil_spill_count,
        "average_confidence": round(avg_confidence, 3),
        "recent_predictions_24h": recent_predictions
    }

@router.get("/{prediction_id}/image")
def get_prediction_image(prediction_id: int, db: Session = Depends(get_db)):
    """Serve prediction image file, or an SVG placeholder if missing (empty sentinel_data)."""
    prediction = db.query(Prediction).filter(Prediction.id == prediction_id).first()
    if not prediction:
        return prediction_image_placeholder()

    path = _resolved_prediction_image_path(prediction.prediction_image_path)
    if not path:
        return prediction_image_placeholder()

    if not os.path.isfile(path):
        return prediction_image_placeholder()

    return FileResponse(
        path,
        media_type="image/jpeg",
        filename=f"prediction_{prediction_id}.jpg",
    )