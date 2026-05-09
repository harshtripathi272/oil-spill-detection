from sqlalchemy import Column, Integer, String, Text, JSON
from sqlalchemy.sql import func
from app.database import Base, UTCDateTime

class LogEntry(Base):
    __tablename__ = "logs"

    id = Column(Integer, primary_key=True, index=True)
    timestamp = Column(UTCDateTime(timezone=True), server_default=func.now())
    level = Column(String, nullable=False)  # INFO, ERROR, WARNING, DEBUG
    service = Column(String, nullable=False)  # kafka, trigger_bridge, ingestion, etc.
    message = Column(Text, nullable=False)
    extra_metadata = Column(JSON, nullable=True)  # Additional log data