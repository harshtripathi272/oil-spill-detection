from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import FileResponse
from sqlalchemy.orm import Session
from typing import List, Optional
from datetime import datetime, timedelta
import os
import glob
from app.database import get_db
from app.models.logs import LogEntry
from app.schemas.logs import LogEntry as LogEntrySchema, LogEntryCreate

router = APIRouter()

LOGS_DIR = "/data/user13/oilspill_ugq/oil-spill-detection/logs"

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