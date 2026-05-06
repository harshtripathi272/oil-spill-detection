from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from app.database import get_db
from app.services.system_service import build_system_health, build_system_resources

router = APIRouter()

@router.get("/health")
async def get_system_health(db: Session = Depends(get_db)):
    """Get overall system health status"""
    return build_system_health(db)

@router.get("/resources")
async def get_system_resources():
    """Get detailed system resource information"""
    return build_system_resources()

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