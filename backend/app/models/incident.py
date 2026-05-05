from sqlalchemy import Column, Integer, String, DateTime, Float, Text, Boolean, ForeignKey, JSON
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from app.database import Base

class Incident(Base):
    __tablename__ = "incidents"

    id = Column(String, primary_key=True, index=True)
    latitude = Column(Float, nullable=False)
    longitude = Column(Float, nullable=False)
    confidence_score = Column(Float, nullable=True)
    detection_time = Column(DateTime(timezone=True), server_default=func.now())
    status = Column(String, default="detected")  # detected, confirmed, false_positive, resolved
    bbox_coordinates = Column(JSON, nullable=True)  # Store bounding box as JSON
    sar_image_path = Column(String, nullable=True)
    processed_image_path = Column(String, nullable=True)
    model_version = Column(String, nullable=True)
    processing_time = Column(Float, nullable=True)  # Time taken for processing in seconds
    metadata = Column(JSON, nullable=True)  # Additional metadata

    # Relationships
    dag_runs = relationship("DagRun", back_populates="incident")

class DagRun(Base):
    __tablename__ = "dag_runs"

    id = Column(Integer, primary_key=True, index=True)
    dag_id = Column(String, nullable=False)
    run_id = Column(String, nullable=False, unique=True)
    incident_id = Column(String, ForeignKey("incidents.id"), nullable=True)
    state = Column(String, nullable=False)  # success, failed, running, etc.
    start_date = Column(DateTime(timezone=True), nullable=True)
    end_date = Column(DateTime(timezone=True), nullable=True)
    execution_time = Column(Float, nullable=True)  # Total execution time in seconds

    # Relationships
    incident = relationship("Incident", back_populates="dag_runs")
    task_instances = relationship("TaskInstance", back_populates="dag_run")

class TaskInstance(Base):
    __tablename__ = "task_instances"

    id = Column(Integer, primary_key=True, index=True)
    dag_run_id = Column(Integer, ForeignKey("dag_runs.id"))
    task_id = Column(String, nullable=False)
    state = Column(String, nullable=False)
    start_date = Column(DateTime(timezone=True), nullable=True)
    end_date = Column(DateTime(timezone=True), nullable=True)
    duration = Column(Float, nullable=True)

    # Relationships
    dag_run = relationship("DagRun", back_populates="task_instances")

class Metric(Base):
    __tablename__ = "metrics"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, nullable=False)
    value = Column(Float, nullable=False)
    timestamp = Column(DateTime(timezone=True), server_default=func.now())
    category = Column(String, nullable=False)  # system, model, processing, etc.
    metadata = Column(JSON, nullable=True)

class SystemStatus(Base):
    __tablename__ = "system_status"

    id = Column(Integer, primary_key=True, index=True)
    component = Column(String, nullable=False)
    status = Column(String, nullable=False)  # healthy, warning, error
    last_check = Column(DateTime(timezone=True), server_default=func.now())
    details = Column(JSON, nullable=True)