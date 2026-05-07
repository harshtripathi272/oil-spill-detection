import os
import psutil
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


@router.get("/services-live")
async def get_services_live():
    """Get live status of known backend services by inspecting running processes."""
    service_keywords = {
        "FastAPI Backend": ["uvicorn"],
        "Anomaly Detector": ["anomaly_detector"],
        "Stream Processor": ["stream_processor"],
        "Trigger Bridge": ["trigger_bridge"],
        "Kafka Broker": ["kafka.Kafka", "kafka-server"],
        "Airflow Scheduler": ["airflow", "scheduler"],
    }

    services = []
    for name, keywords in service_keywords.items():
        found = False
        svc_cpu = 0.0
        svc_mem = 0.0
        svc_start = None
        for proc in psutil.process_iter(["pid", "name", "cmdline", "cpu_percent", "memory_info", "create_time"]):
            try:
                cmdline = " ".join(proc.info.get("cmdline") or [])
                if any(kw in cmdline for kw in keywords):
                    found = True
                    svc_cpu += proc.info.get("cpu_percent", 0) or 0
                    mem_info = proc.info.get("memory_info")
                    if mem_info:
                        svc_mem += mem_info.rss
                    ct = proc.info.get("create_time")
                    if ct and (svc_start is None or ct < svc_start):
                        svc_start = ct
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                continue

        import time
        uptime_str = ""
        if svc_start:
            up_secs = time.time() - svc_start
            days = int(up_secs // 86400)
            hours = int((up_secs % 86400) // 3600)
            mins = int((up_secs % 3600) // 60)
            uptime_str = f"{days}d {hours:02d}h {mins:02d}m"

        services.append({
            "name": name,
            "status": "Healthy" if found else "Stopped",
            "running": found,
            "cpu_percent": round(svc_cpu, 1),
            "memory_mb": round(svc_mem / (1024 * 1024), 1) if svc_mem else 0,
            "uptime": uptime_str,
        })

    return {"services": services}