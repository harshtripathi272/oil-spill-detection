from sqlalchemy import Column, Integer, String, Float, Text, JSON, ForeignKey
from sqlalchemy.sql import func
from app.database import Base, UTCDateTime

class Prediction(Base):
    __tablename__ = "predictions"

    id = Column(Integer, primary_key=True, index=True)
    incident_id = Column(String, ForeignKey("incidents.id"), nullable=True)
    dag_run_id = Column(String, nullable=True)  # Reference to Airflow DAG run
    image_path = Column(String, nullable=False)  # Path to the original SAR image
    prediction_image_path = Column(String, nullable=True)  # Path to the prediction image with bounding boxes
    prediction = Column(String, nullable=False)  # oil_spill, no_oil_spill, etc.
    confidence = Column(Float, nullable=False)
    bbox_coordinates = Column(JSON, nullable=True)  # Bounding box coordinates
    mask_path = Column(String, nullable=True)  # Path to segmentation mask if available
    model_version = Column(String, nullable=True)
    processing_time = Column(Float, nullable=True)  # Time taken for inference
    created_at = Column(UTCDateTime(timezone=True), server_default=func.now())
    extra_metadata = Column(JSON, nullable=True)  # Additional prediction metadata