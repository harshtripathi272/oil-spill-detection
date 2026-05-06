import os
import psutil
import subprocess
from datetime import datetime
from sqlalchemy.orm import Session
from app.schemas.dashboard import SystemHealth, SystemStatus
from app.config import settings


def check_kafka_status() -> SystemStatus:
    """Check Kafka broker status"""
    try:
        # Check if Kafka process is running
        result = subprocess.run(
            ["pgrep", "-f", "kafka"],
            capture_output=True,
            text=True,
            timeout=5
        )
        kafka_running = result.returncode == 0 and result.stdout.strip()

        # Try to connect to Kafka
        from kafka import KafkaAdminClient
        try:
            admin_client = KafkaAdminClient(
                bootstrap_servers=settings.kafka_bootstrap_servers,
                client_id='health-check'
            )
            topics = admin_client.list_topics()
            admin_client.close()
            kafka_status = "healthy"
            kafka_details = {
                "topics_count": len(topics),
                "topics": list(topics)[:5],  # Show first 5 topics
                "bootstrap_servers": settings.kafka_bootstrap_servers
            }
        except Exception as e:
            kafka_status = "warning" if kafka_running else "error"
            kafka_details = {
                "error": str(e),
                "process_running": bool(kafka_running)
            }

    except Exception as e:
        kafka_status = "error"
        kafka_details = {"error": f"Could not check Kafka: {str(e)}"}

    return SystemStatus(
        component="kafka",
        status=kafka_status,
        last_check=datetime.utcnow(),
        details=kafka_details
    )


def check_airflow_status() -> SystemStatus:
    """Check Airflow components status"""
    try:
        # Check Airflow processes
        result = subprocess.run(
            ["pgrep", "-f", "airflow"],
            capture_output=True,
            text=True,
            timeout=5
        )

        processes = result.stdout.strip().split('\n') if result.stdout.strip() else []
        processes = [p for p in processes if p]  # Filter empty strings

        scheduler_running = any("scheduler" in proc for proc in processes)
        triggerer_running = any("triggerer" in proc for proc in processes)
        webserver_running = any("webserver" in proc for proc in processes)

        # Determine overall status
        components_running = sum([scheduler_running, triggerer_running, webserver_running])
        if components_running == 3:
            airflow_status = "healthy"
        elif components_running > 0:
            airflow_status = "warning"
        else:
            airflow_status = "error"

        details = {
            "scheduler": "running" if scheduler_running else "stopped",
            "triggerer": "running" if triggerer_running else "stopped",
            "webserver": "running" if webserver_running else "stopped",
            "total_processes": len(processes),
            "dags_folder": settings.airflow_dags_folder
        }

    except Exception as e:
        airflow_status = "error"
        details = {"error": f"Could not check Airflow: {str(e)}"}

    return SystemStatus(
        component="airflow",
        status=airflow_status,
        last_check=datetime.utcnow(),
        details=details
    )


def check_model_server_status() -> SystemStatus:
    """Check if model server/API is running"""
    try:
        # Check if our own API is accessible (self-check)
        import requests
        response = requests.get("http://localhost:8000/health", timeout=5)
        if response.status_code == 200:
            api_status = "healthy"
            details = {"response_time": response.elapsed.total_seconds()}
        else:
            api_status = "error"
            details = {"status_code": response.status_code}
    except Exception as e:
        api_status = "error"
        details = {"error": f"Could not connect to API: {str(e)}"}

    return SystemStatus(
        component="api_server",
        status=api_status,
        last_check=datetime.utcnow(),
        details=details
    )


def check_log_files_status() -> SystemStatus:
    """Check log files status"""
    logs_dir = "/data/user13/oilspill_ugq/oil-spill-detection/logs"
    try:
        if not os.path.exists(logs_dir):
            return SystemStatus(
                component="logs",
                status="error",
                last_check=datetime.utcnow(),
                details={"error": "Logs directory not found"}
            )

        log_files = [f for f in os.listdir(logs_dir) if f.endswith('.log')]
        total_size = 0
        file_info = []

        for log_file in log_files:
            file_path = os.path.join(logs_dir, log_file)
            try:
                stat = os.stat(file_path)
                total_size += stat.st_size
                file_info.append({
                    "name": log_file,
                    "size": stat.st_size,
                    "modified": datetime.fromtimestamp(stat.st_mtime).isoformat()
                })
            except OSError:
                continue

        # Check for recent activity (files modified in last hour)
        recent_activity = any(
            (datetime.now() - datetime.fromtimestamp(stat.st_mtime)).seconds < 3600
            for stat in [os.stat(os.path.join(logs_dir, f)) for f in log_files]
        )

        status = "healthy" if recent_activity else "warning"

        return SystemStatus(
            component="logs",
            status=status,
            last_check=datetime.utcnow(),
            details={
                "total_files": len(log_files),
                "total_size": total_size,
                "recent_activity": recent_activity,
                "files": file_info[:5]  # Show first 5 files
            }
        )

    except Exception as e:
        return SystemStatus(
            component="logs",
            status="error",
            last_check=datetime.utcnow(),
            details={"error": f"Could not check logs: {str(e)}"}
        )


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
        ),
        check_kafka_status(),
        check_airflow_status(),
        check_model_server_status(),
        check_log_files_status()
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
