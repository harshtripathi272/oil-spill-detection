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

from app.database import SessionLocal, engine, Base
from app.models.incident import Incident, DagRun, TaskInstance, Metric, SystemStatus
from app.models.alerts import Alert
from app.models.users import User
from app.models.logs import LogEntry
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

def create_sample_alerts(db: Session, incidents):
    """Create sample alerts"""
    print("Creating sample alerts...")

    alerts_data = [
        {
            "incident_id": incidents[0].id,
            "level": "high",
            "message": "High confidence oil spill detected in Gulf of Mexico",
            "acknowledged": False
        },
        {
            "incident_id": incidents[1].id,
            "level": "medium",
            "message": "Potential oil spill detected near Houston",
            "acknowledged": True
        },
        {
            "incident_id": None,
            "level": "low",
            "message": "System maintenance scheduled for tonight",
            "acknowledged": False
        },
        {
            "incident_id": incidents[2].id,
            "level": "high",
            "message": "Confirmed oil spill in New York Harbor",
            "acknowledged": True
        }
    ]

    alerts = []
    for data in alerts_data:
        alert = Alert(**data)
        alerts.append(alert)
        db.add(alert)

    db.commit()
    print(f"Created {len(alerts)} sample alerts")
    return alerts

def create_sample_users(db: Session):
    """Create sample users"""
    print("Creating sample users...")

    users_data = [
        {
            "username": "admin",
            "full_name": "System Administrator",
            "email": "admin@oilspill.gov",
            "role": "admin",
            "hashed_password": "$2b$12$LQv3c1yqBWVHxkd0LHAkCOYz6TtxMQJqhN8/LewdBPj6fMJyHnUe",  # password: admin123
            "enabled": True
        },
        {
            "username": "analyst1",
            "full_name": "John Analyst",
            "email": "john.analyst@oilspill.gov",
            "role": "analyst",
            "hashed_password": "$2b$12$LQv3c1yqBWVHxkd0LHAkCOYz6TtxMQJqhN8/LewdBPj6fMJyHnUe",  # password: analyst123
            "enabled": True
        },
        {
            "username": "analyst2",
            "full_name": "Jane Analyst",
            "email": "jane.analyst@oilspill.gov",
            "role": "analyst",
            "hashed_password": "$2b$12$LQv3c1yqBWVHxkd0LHAkCOYz6TtxMQJqhN8/LewdBPj6fMJyHnUe",  # password: analyst123
            "enabled": True
        }
    ]

    users = []
    for data in users_data:
        user = User(**data)
        users.append(user)
        db.add(user)

    db.commit()
    print(f"Created {len(users)} sample users")
    return users

def create_sample_system_status(db: Session):
    """Create sample system status"""
    print("Creating sample system status...")

    status_data = [
        {
            "component": "database",
            "status": "healthy",
            "details": {"connection": "ok", "latency": "5ms"}
        },
        {
            "component": "kafka",
            "status": "healthy",
            "details": {"brokers": 3, "topics": 5}
        },
        {
            "component": "airflow",
            "status": "warning",
            "details": {"dags": 2, "running": 1, "failed": 0}
        },
        {
            "component": "model_server",
            "status": "healthy",
            "details": {"version": "yolo26n-bbox-1024-merged", "uptime": "24h"}
        },
        {
            "component": "api_server",
            "status": "healthy",
            "details": {"requests_per_minute": 45, "error_rate": 0.1}
        }
    ]

    statuses = []
    for data in status_data:
        status = SystemStatus(**data)
        statuses.append(status)
        db.add(status)

    db.commit()
    print(f"Created {len(statuses)} sample system status entries")
    return statuses

def create_sample_logs(db: Session):
    """Create sample log entries"""
    print("Creating sample logs...")

    log_entries = [
        {
            "level": "INFO",
            "service": "kafka",
            "message": "Kafka broker started successfully on localhost:9092",
            "extra_metadata": {"broker_id": 1, "topics": ["sar-trigger-events"]}
        },
        {
            "level": "INFO",
            "service": "trigger_bridge",
            "message": "SAR image fetch triggered for incident INC001",
            "extra_metadata": {"incident_id": "incident_001", "image_url": "https://example.com/sar/image1.tif"}
        },
        {
            "level": "INFO",
            "service": "ingestion",
            "message": "Successfully ingested SAR image for processing",
            "extra_metadata": {"file_size": "45MB", "processing_time": "2.3s"}
        },
        {
            "level": "WARNING",
            "service": "anomaly_detector",
            "message": "High confidence anomaly detected, confidence: 0.89",
            "extra_metadata": {"confidence": 0.89, "bbox": [100, 200, 300, 400]}
        },
        {
            "level": "INFO",
            "service": "airflow",
            "message": "DAG suspicious_event_validation completed successfully",
            "extra_metadata": {"dag_id": "suspicious_event_validation", "run_id": "manual__2024-01-01T10:00:00", "duration": "45.2s"}
        },
        {
            "level": "ERROR",
            "service": "stream_processor",
            "message": "Failed to process message from topic sar-trigger-events",
            "extra_metadata": {"error": "Connection timeout", "retry_count": 3}
        },
        {
            "level": "INFO",
            "service": "api_server",
            "message": "Incident incident_001 status updated to confirmed",
            "extra_metadata": {"incident_id": "incident_001", "old_status": "detected", "new_status": "confirmed"}
        }
    ]

    logs = []
    for log_data in log_entries:
        # Randomize timestamp within last 24 hours
        timestamp = datetime.utcnow() - timedelta(hours=random.randint(0, 24))
        log = LogEntry(
            **log_data,
            timestamp=timestamp
        )
        logs.append(log)
        db.add(log)

    db.commit()
    print(f"Created {len(logs)} sample log entries")
    return logs

def main():
    """Main seeding function"""
    print("🌱 Seeding Oil Spill Detection Backend Database")
    print("=" * 50)

    # Create all tables
    print("Creating database tables...")
    Base.metadata.create_all(bind=engine)
    print("✅ Tables created")

    db = SessionLocal()

    try:
        # Create sample data
        incidents = create_sample_incidents(db)
        create_sample_dag_runs(db, incidents)
        create_sample_metrics(db)
        create_sample_alerts(db, incidents)
        create_sample_users(db)
        create_sample_system_status(db)
        create_sample_logs(db)

        print("=" * 50)
        print("✅ Database seeding completed successfully!")
        print("\nSample data created:")
        print(f"  • {len(incidents)} incidents")
        print("  • Corresponding DAG runs")
        print("  • 7 days of metrics data")
        print("  • Sample alerts")
        print("  • Sample users")
        print("  • System status entries")
        print("  • Sample log entries")
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