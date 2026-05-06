from sqlalchemy import Column, Integer, String, DateTime, Text, JSON
from sqlalchemy.sql import func
from app.database import Base

class LogEntry(Base):
    __tablename__ = "logs"

    id = Column(Integer, primary_key=True, index=True)
    timestamp = Column(DateTime(timezone=True), server_default=func.now())
    level = Column(String, nullable=False)  # INFO, ERROR, WARNING, DEBUG
    service = Column(String, nullable=False)  # kafka, trigger_bridge, ingestion, etc.
    message = Column(Text, nullable=False)
    extra_metadata = Column(JSON, nullable=True)  # Additional log data