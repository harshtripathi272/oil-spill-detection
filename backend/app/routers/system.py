from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from typing import List
import psutil
import os
from datetime import datetime
from app.database import get_db
from app.schemas.dashboard import SystemStatus, SystemHealth

router = APIRouter()

@router.get("/health", response_model=SystemHealth)
async def get_system_health(db: Session = Depends(get_db)):
    """Get overall system health status"""
    # Check database connectivity
    try:
        db.execute("SELECT 1")
        db_status = "healthy"
    except Exception:
        db_status = "error"

    # Check disk usage
    disk_usage = psutil.disk_usage('/')
    disk_status = "healthy" if disk_usage.percent < 90 else "warning" if disk_usage.percent < 95 else "error"

    # Check memory usage
    memory = psutil.virtual_memory()
    memory_status = "healthy" if memory.percent < 80 else "warning" if memory.percent < 90 else "error"

    # Check CPU usage
    cpu_percent = psutil.cpu_percent(interval=1)
    cpu_status = "healthy" if cpu_percent < 70 else "warning" if cpu_percent < 85 else "error"

    # Get system uptime
    uptime = datetime.now().timestamp() - psutil.boot_time()

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

    # Determine overall status
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
        uptime=uptime,
        last_updated=datetime.utcnow()
    )

@router.get("/resources")
async def get_system_resources():
    """Get detailed system resource information"""
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

@router.get("/processes")
async def get_system_processes():
    """Get information about running processes"""
    processes = []
    for proc in psutil.process_iter(['pid', 'name', 'cpu_percent', 'memory_percent', 'status']):
        try:
            info = proc.info
            processes.append({
                "pid": info['pid'],
                "name": info['name'],
                "cpu_percent": info['cpu_percent'],
                "memory_percent": info['memory_percent'],
                "status": info['status']
            })
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            continue

    # Sort by memory usage and return top 10
    processes.sort(key=lambda x: x['memory_percent'], reverse=True)
    return {"processes": processes[:10]}

@router.get("/airflow/status")
async def get_airflow_status():
    """Get Airflow system status"""
    import subprocess
    import os

    try:
        # Check if airflow processes are running
        result = subprocess.run(
            ["pgrep", "-f", "airflow"],
            capture_output=True,
            text=True
        )

        processes = result.stdout.strip().split('\n') if result.stdout.strip() else []

        # Try to get scheduler status
        scheduler_running = any("scheduler" in proc for proc in processes if proc)
        triggerer_running = any("triggerer" in proc for proc in processes if proc)
        webserver_running = any("webserver" in proc for proc in processes if proc)

        return {
            "scheduler": "running" if scheduler_running else "stopped",
            "triggerer": "running" if triggerer_running else "stopped",
            "webserver": "running" if webserver_running else "stopped",
            "total_processes": len([p for p in processes if p])
        }

    except Exception as e:
        return {
            "error": f"Could not check Airflow status: {str(e)}",
            "scheduler": "unknown",
            "triggerer": "unknown",
            "webserver": "unknown"
        }

@router.get("/logs/recent")
async def get_recent_logs(lines: int = 50):
    """Get recent system logs"""
    import subprocess

    try:
        # Get recent syslog entries
        result = subprocess.run(
            ["tail", "-n", str(lines), "/var/log/syslog"],
            capture_output=True,
            text=True
        )

        if result.returncode == 0:
            logs = result.stdout.strip().split('\n')
            return {"logs": logs}
        else:
            return {"error": "Could not read system logs", "logs": []}

    except Exception as e:
        return {"error": f"Could not read logs: {str(e)}", "logs": []}

@router.get("/config")
async def get_system_config():
    """Get system configuration information"""
    return {
        "python_version": os.sys.version,
        "platform": os.sys.platform,
        "environment_variables": {
            key: value for key, value in os.environ.items()
            if not any(secret in key.lower() for secret in ['password', 'secret', 'key', 'token'])
        },
        "working_directory": os.getcwd(),
        "process_id": os.getpid()
    }