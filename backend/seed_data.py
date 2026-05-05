#!/usr/bin/env python3
"""
Data seeding script for Oil Spill Detection Backend
Populates the database with sample data for testing and development
"""

import sys
import os
from datetime import datetime, timedelta
import random

# Add the app directory to the path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from app.database import SessionLocal, engine
from app.models.incident import Incident, DagRun, TaskInstance, Metric
from sqlalchemy.orm import Session

def create_sample_incidents(db: Session):
    """Create sample incidents"""
    print("Creating sample incidents...")

    # Sample incident data
    incidents_data = [
        {
            "id": "incident_001",
            "latitude": 28.5383,
            "longitude": -88.0717,
            "confidence_score": 0.85,
            "status": "confirmed",
            "bbox_coordinates": {"x1": 100, "y1": 200, "x2": 300, "y2": 400},
            "model_version": "yolo26n-bbox-1024-merged",
            "processing_time": 45.2
        },
        {
            "id": "incident_002",
            "latitude": 29.7604,
            "longitude": -95.3698,
            "confidence_score": 0.72,
            "status": "detected",
            "bbox_coordinates": {"x1": 150, "y1": 250, "x2": 350, "y2": 450},
            "model_version": "yolo26n-bbox-1024-merged",
            "processing_time": 38.7
        },
        {
            "id": "incident_003",
            "latitude": 40.7128,
            "longitude": -74.0060,
            "confidence_score": 0.91,
            "status": "resolved",
            "bbox_coordinates": {"x1": 200, "y1": 300, "x2": 400, "y2": 500},
            "model_version": "yolo26n-bbox-1024-merged",
            "processing_time": 52.1
        },
        {
            "id": "incident_004",
            "latitude": 34.0522,
            "longitude": -118.2437,
            "confidence_score": 0.68,
            "status": "false_positive",
            "bbox_coordinates": {"x1": 120, "y1": 180, "x2": 280, "y2": 380},
            "model_version": "yolo26n-bbox-1024-merged",
            "processing_time": 41.8
        },
        {
            "id": "incident_005",
            "latitude": 41.8781,
            "longitude": -87.6298,
            "confidence_score": 0.79,
            "status": "confirmed",
            "bbox_coordinates": {"x1": 180, "y1": 220, "x2": 380, "y2": 420},
            "model_version": "yolo26n-bbox-1024-merged",
            "processing_time": 47.3
        }
    ]

    incidents = []
    for data in incidents_data:
        # Randomize detection time within last 30 days
        days_ago = random.randint(0, 30)
        detection_time = datetime.utcnow() - timedelta(days=days_ago, hours=random.randint(0, 23))

        incident = Incident(
            **data,
            detection_time=detection_time
        )
        incidents.append(incident)
        db.add(incident)

    db.commit()
    print(f"Created {len(incidents)} sample incidents")
    return incidents

def create_sample_dag_runs(db: Session, incidents):
    """Create sample DAG runs"""
    print("Creating sample DAG runs...")

    dag_runs = []
    for incident in incidents:
        # Create a DAG run for each incident
        run_id = f"manual__{incident.detection_time.strftime('%Y-%m-%dT%H:%M:%S')}"
        dag_run = DagRun(
            dag_id="suspicious_event_validation",
            run_id=run_id,
            incident_id=incident.id,
            state="success" if incident.status in ["confirmed", "resolved"] else "running",
            start_date=incident.detection_time,
            end_date=incident.detection_time + timedelta(seconds=incident.processing_time or 0),
            execution_time=incident.processing_time
        )
        dag_runs.append(dag_run)
        db.add(dag_run)

    db.commit()
    print(f"Created {len(dag_runs)} sample DAG runs")
    return dag_runs

def create_sample_metrics(db: Session):
    """Create sample metrics"""
    print("Creating sample metrics...")

    # System metrics
    system_metrics = [
        ("cpu_usage", 45.2, "system"),
        ("memory_usage", 67.8, "system"),
        ("disk_usage", 34.1, "system"),
        ("network_io", 1024.5, "system")
    ]

    # Model metrics
    model_metrics = [
        ("accuracy", 0.89, "model"),
        ("precision", 0.85, "model"),
        ("recall", 0.91, "model"),
        ("f1_score", 0.88, "model")
    ]

    # Processing metrics
    processing_metrics = [
        ("avg_processing_time", 42.3, "processing"),
        ("total_processed", 150, "processing"),
        ("success_rate", 94.2, "processing")
    ]

    all_metrics = system_metrics + model_metrics + processing_metrics
    metrics = []

    for name, value, category in all_metrics:
        # Create metrics for the last 7 days
        for i in range(7):
            timestamp = datetime.utcnow() - timedelta(days=i, hours=random.randint(0, 23))
            # Add some variation to the values
            varied_value = value + random.uniform(-5, 5)

            metric = Metric(
                name=name,
                value=round(varied_value, 2),
                category=category,
                timestamp=timestamp
            )
            metrics.append(metric)
            db.add(metric)

    db.commit()
    print(f"Created {len(metrics)} sample metrics")

def main():
    """Main seeding function"""
    print("🌱 Seeding Oil Spill Detection Backend Database")
    print("=" * 50)

    db = SessionLocal()

    try:
        # Create sample data
        incidents = create_sample_incidents(db)
        create_sample_dag_runs(db, incidents)
        create_sample_metrics(db)

        print("=" * 50)
        print("✅ Database seeding completed successfully!")
        print("\nSample data created:")
        print(f"  • {len(incidents)} incidents")
        print("  • Corresponding DAG runs")
        print("  • 7 days of metrics data")
        print("\nYou can now start the backend server and test the APIs!")

    except Exception as e:
        print(f"❌ Error during seeding: {e}")
        db.rollback()
        return 1
    finally:
        db.close()

    return 0

if __name__ == "__main__":
    exit(main())