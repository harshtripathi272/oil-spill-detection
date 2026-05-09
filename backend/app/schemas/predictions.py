from pydantic import BaseModel, Field
from typing import Optional, Dict, Any
from datetime import datetime

class PredictionBase(BaseModel):
    incident_id: Optional[str] = None
    dag_run_id: Optional[str] = None
    image_path: str
    prediction_image_path: Optional[str] = None
    prediction: str
    confidence: float = Field(..., ge=0.0, le=1.0)
    bbox_coordinates: Optional[Dict[str, Any]] = None
    mask_path: Optional[str] = None
    model_version: Optional[str] = None
    processing_time: Optional[float] = None
    extra_metadata: Optional[Dict[str, Any]] = None

class PredictionCreate(PredictionBase):
    pass

class PredictionUpdate(BaseModel):
    incident_id: Optional[str] = None
    dag_run_id: Optional[str] = None
    prediction_image_path: Optional[str] = None
    prediction: Optional[str] = None
    confidence: Optional[float] = Field(None, ge=0.0, le=1.0)
    bbox_coordinates: Optional[Dict[str, Any]] = None
    mask_path: Optional[str] = None
    model_version: Optional[str] = None
    processing_time: Optional[float] = None
    extra_metadata: Optional[Dict[str, Any]] = None

class PredictionResponse(PredictionBase):
    id: int
    created_at: datetime

    class Config:
        from_attributes = True