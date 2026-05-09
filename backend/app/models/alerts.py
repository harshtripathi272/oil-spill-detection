from sqlalchemy import Column, Integer, String, Boolean, JSON, ForeignKey
from sqlalchemy.sql import func
from app.database import Base, UTCDateTime

class Alert(Base):
    __tablename__ = "alerts"

    id = Column(Integer, primary_key=True, index=True)
    incident_id = Column(String, ForeignKey("incidents.id"), nullable=True)
    level = Column(String, default="medium")
    message = Column(String, nullable=False)
    created_at = Column(UTCDateTime(timezone=True), server_default=func.now())
    acknowledged = Column(Boolean, default=False)
    extra_metadata = Column(JSON, nullable=True)
