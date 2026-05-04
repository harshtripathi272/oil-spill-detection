import logging
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from app.config import settings
from app.db.supabase import SupabaseStore

logger = logging.getLogger(__name__)

# Create engine for SQLAlchemy fallback or Supabase Postgres URL
engine = create_engine(
    settings.database_url,
    pool_pre_ping=True,
    echo=settings.debug,
    future=True,
)

# Create SessionLocal class
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# Create Base class
Base = declarative_base()

# Supabase store for the backend
supabase = SupabaseStore()

# Dependency to get DB session
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()