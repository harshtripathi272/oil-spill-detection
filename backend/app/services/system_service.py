import os
import psutil
from datetime import datetime
from sqlalchemy.orm import Session
from app.schemas.dashboard import SystemHealth, SystemStatus


def build_system_health(db: Session) -> SystemHealth:
    try:
        db.execute("SELECT 1")
        db_status = "healthy"
    except Exception:
        db_status = "error"

    disk_usage = psutil.disk_usage('/')
    disk_status = "healthy" if disk_usage.percent < 90 else "warning" if disk_usage.percent < 95 else "error"

    memory = psutil.virtual_memory()
    memory_status = "healthy" if memory.percent < 80 else "warning" if memory.percent < 90 else "error"

    cpu_percent = psutil.cpu_percent(interval=1)
    cpu_status = "healthy" if cpu_percent < 70 else "warning" if cpu_percent < 85 else "error"

    components = [
        SystemStatus(
            component="database",
            status=db_status,
            last_check=datetime.utcnow(),
            details={"connection": "postgresql" if "postgresql" in os.getenv("DATABASE_URL", "") else "sqlite"}
        ),
        SystemStatus(
            component="disk",
            status=disk_status,
            last_check=datetime.utcnow(),
            details={
                "total": disk_usage.total,
                "used": disk_usage.used,
                "free": disk_usage.free,
                "percent": disk_usage.percent
            }
        ),
        SystemStatus(
            component="memory",
            status=memory_status,
            last_check=datetime.utcnow(),
            details={
                "total": memory.total,
                "available": memory.available,
                "percent": memory.percent
            }
        ),
        SystemStatus(
            component="cpu",
            status=cpu_status,
            last_check=datetime.utcnow(),
            details={"usage_percent": cpu_percent}
        )
    ]

    statuses = [comp.status for comp in components]
    if "error" in statuses:
        overall_status = "error"
    elif "warning" in statuses:
        overall_status = "warning"
    else:
        overall_status = "healthy"

    return SystemHealth(
        overall_status=overall_status,
        components=components,
        uptime=datetime.now().timestamp() - psutil.boot_time(),
        last_updated=datetime.utcnow()
    )


def build_system_resources() -> dict:
    return {
        "cpu": {
            "cores": psutil.cpu_count(),
            "usage_percent": psutil.cpu_percent(interval=0.1),
            "frequency": psutil.cpu_freq().current if psutil.cpu_freq() else None
        },
        "memory": {
            "total": psutil.virtual_memory().total,
            "available": psutil.virtual_memory().available,
            "used": psutil.virtual_memory().used,
            "percent": psutil.virtual_memory().percent
        },
        "disk": {
            "total": psutil.disk_usage('/').total,
            "used": psutil.disk_usage('/').used,
            "free": psutil.disk_usage('/').free,
            "percent": psutil.disk_usage('/').percent
        },
        "network": {
            "bytes_sent": psutil.net_io_counters().bytes_sent,
            "bytes_recv": psutil.net_io_counters().bytes_recv,
            "packets_sent": psutil.net_io_counters().packets_sent,
            "packets_recv": psutil.net_io_counters().packets_recv
        }
    }
