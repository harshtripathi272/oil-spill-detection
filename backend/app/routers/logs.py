from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import FileResponse, StreamingResponse
from sqlalchemy.orm import Session
from typing import List, Optional
from datetime import datetime, timedelta
import asyncio
import os
import glob
from app.database import get_db
from app.models.logs import LogEntry
from app.schemas.logs import LogEntry as LogEntrySchema, LogEntryCreate

router = APIRouter()

LOGS_DIR = "/data/user13/oilspill_ugq/oil-spill-detection/logs"

# Services we stream live logs for
KNOWN_SERVICES = {
    "anomaly_detector": "anomaly_detector.log",
    "ingestion":        "ingestion.log",
    "stream_processor": "stream_processor.log",
    "trigger_bridge":   "trigger_bridge.log",
}

@router.get("/stream")
async def stream_logs(
    services: str = Query(
        "anomaly_detector,ingestion,stream_processor,trigger_bridge",
        description="Comma-separated list of service names to stream"
    ),
    tail: int = Query(20, description="Lines to send on connect (per service)"),
):
    """
    SSE endpoint that streams new log lines from one or more service log files.
    Connect once — the server pushes new lines as they appear.
    Format:  data: {"service":"…","line":"…","ts":"…"}\\n\\n
    """
    svc_list = [s.strip() for s in services.split(",") if s.strip() in KNOWN_SERVICES]
    if not svc_list:
        svc_list = list(KNOWN_SERVICES.keys())

    async def event_generator():
        # Track byte offsets per file so we only send new content
        file_positions: dict[str, int] = {}

        for svc in svc_list:
            path = os.path.join(LOGS_DIR, KNOWN_SERVICES[svc])
            if not os.path.exists(path):
                continue
            # Send `tail` most recent lines immediately on connect
            with open(path, "r", encoding="utf-8", errors="ignore") as f:
                all_lines = f.readlines()
                recent = all_lines[-tail:]
                file_positions[svc] = f.tell()
            for line in recent:
                line = line.rstrip()
                if line:
                    ts = line[:19] if len(line) > 19 else ""
                    yield (
                        f"data: {{\"service\":\"{svc}\",\"line\":{line!r},\"ts\":{ts!r}}}\n\n"
                    )

        # Then tail new lines in a loop
        while True:
            for svc in svc_list:
                path = os.path.join(LOGS_DIR, KNOWN_SERVICES[svc])
                if not os.path.exists(path):
                    continue
                try:
                    with open(path, "r", encoding="utf-8", errors="ignore") as f:
                        f.seek(file_positions.get(svc, 0))
                        new_lines = f.readlines()
                        file_positions[svc] = f.tell()
                    for line in new_lines:
                        line = line.rstrip()
                        if line:
                            ts = line[:19] if len(line) > 19 else ""
                            yield (
                                f"data: {{\"service\":\"{svc}\",\"line\":{line!r},\"ts\":{ts!r}}}\n\n"
                            )
                except Exception:
                    pass
            await asyncio.sleep(2)

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
        },
    )



@router.get("/files", response_model=List[dict])
def get_log_files():
    """Get list of available log files"""
    if not os.path.exists(LOGS_DIR):
        return []

    log_files = []
    for file_path in glob.glob(os.path.join(LOGS_DIR, "*.log")):
        filename = os.path.basename(file_path)
        try:
            stat = os.stat(file_path)
            log_files.append({
                "filename": filename,
                "service": filename.replace(".log", ""),
                "size": stat.st_size,
                "modified": datetime.fromtimestamp(stat.st_mtime).isoformat(),
                "path": file_path
            })
        except OSError:
            continue

    return log_files

@router.get("/files/{filename}/content")
def get_log_file_content(
    filename: str,
    lines: int = Query(100, description="Number of lines to return from the end"),
    search: Optional[str] = Query(None, description="Search term to filter lines")
):
    """Get content from a specific log file"""
    file_path = os.path.join(LOGS_DIR, filename)

    if not os.path.exists(file_path):
        raise HTTPException(status_code=404, detail="Log file not found")

    if not filename.endswith('.log'):
        raise HTTPException(status_code=400, detail="Invalid log file")

    try:
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
            all_lines = f.readlines()

        # Get the last N lines
        lines_content = all_lines[-lines:] if len(all_lines) > lines else all_lines

        # Filter by search term if provided
        if search:
            lines_content = [line for line in lines_content if search.lower() in line.lower()]

        return {
            "filename": filename,
            "total_lines": len(all_lines),
            "returned_lines": len(lines_content),
            "content": "".join(lines_content)
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error reading log file: {str(e)}")

@router.get("/download/{filename}")
def download_log_file(filename: str):
    """Download a log file."""
    file_path = os.path.join(LOGS_DIR, filename)
    if not os.path.exists(file_path) or not filename.endswith('.log'):
        raise HTTPException(status_code=404, detail="Log file not found")
    return FileResponse(
        path=file_path,
        filename=filename,
        media_type='text/plain'
    )

@router.get("/recent")
def get_recent_logs(
    db: Session = Depends(get_db),
    service: Optional[str] = Query(None, description="Filter by service"),
    level: Optional[str] = Query(None, description="Filter by log level"),
    limit: int = Query(50, description="Number of recent logs to return")
):
    """Get recent logs from database"""
    query = db.query(LogEntry).order_by(LogEntry.timestamp.desc())

    if service:
        query = query.filter(LogEntry.service == service)

    if level:
        query = query.filter(LogEntry.level == level)

    logs = query.limit(limit).all()
    return logs

@router.post("/")
def create_log_entry(
    log: LogEntryCreate,
    db: Session = Depends(get_db)
):
    """Create a new log entry (for services to log to database)"""
    db_log = LogEntry(**log.dict())
    db.add(db_log)
    db.commit()
    db.refresh(db_log)
    return db_log