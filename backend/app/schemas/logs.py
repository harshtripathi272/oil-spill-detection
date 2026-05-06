from pydantic import BaseModel
from datetime import datetime
from typing import Optional, Any, Dict

class LogEntryBase(BaseModel):
    level: str
    service: str
    message: str
    extra_metadata: Optional[Dict[str, Any]] = None

class LogEntryCreate(LogEntryBase):
    pass

class LogEntry(LogEntryBase):
    id: int
    timestamp: datetime

    class Config:
        from_attributes = True