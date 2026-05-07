import logging
from sqlalchemy import create_engine, DateTime, String, TypeDecorator
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from app.config import settings
from app.db.supabase import SupabaseStore
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

# Custom DateTime type that handles ISO format strings with Z suffix
class UTCDateTime(TypeDecorator):
    """Handles datetime strings with Z suffix (UTC indicator)"""
    impl = DateTime(timezone=True)
    cache_ok = True

    def load_dialect_impl(self, dialect):
        # SQLite stores datetimes as TEXT and SQLAlchemy's default processor
        # can't parse ISO strings ending in 'Z'. Store as string and parse ourselves.
        if dialect.name == "sqlite":
            return dialect.type_descriptor(String())
        return dialect.type_descriptor(DateTime(timezone=True))

    def process_bind_param(self, value, dialect):
        if value is not None:
            if isinstance(value, str):
                # If it's a string, try to parse it
                value = value.rstrip('Z')
                try:
                    value = datetime.fromisoformat(value)
                except (ValueError, TypeError):
                    return value
            if isinstance(value, datetime):
                # For SQLite, persist as ISO string without timezone + without 'Z'
                if dialect.name == "sqlite":
                    dt = value.astimezone(timezone.utc) if value.tzinfo else value
                    return dt.replace(tzinfo=None).isoformat(timespec="microseconds")
                return value.replace(tzinfo=None) if value.tzinfo else value
        return value

    def process_result_value(self, value, dialect):
        if value is not None:
            if isinstance(value, str):
                # Remove Z suffix if present for fromisoformat compatibility
                clean_value = value.rstrip('Z')
                try:
                    return datetime.fromisoformat(clean_value)
                except (ValueError, TypeError):
                    # Fallback: try to parse with timezone awareness
                    try:
                        if value.endswith('Z'):
                            return datetime.fromisoformat(value[:-1] + '+00:00')
                        return datetime.fromisoformat(value)
                    except (ValueError, TypeError):
                        return None
            elif isinstance(value, datetime):
                return value
        return value

# Create engine for SQLAlchemy fallback or Supabase Postgres URL
engine = create_engine(
    settings.database_url,
    pool_pre_ping=True,
    echo=False,  # Disabled: was flooding terminal and masking real API logs
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