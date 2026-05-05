from pydantic_settings import BaseSettings
from typing import Optional
import os

class Settings(BaseSettings):
    # Database settings
    database_url: str = "sqlite:///./oilspill.db"

    # Supabase settings
    supabase_url: Optional[str] = None
    supabase_service_key: Optional[str] = None

    # Airflow settings
    airflow_db_url: str = "sqlite:////data/user13/airflow/airflow.db"
    airflow_dags_folder: str = "/data/user13/oilspill_ugq/oil-spill-detection/orchestration/dags"

    # API settings
    api_host: str = "0.0.0.0"
    api_port: int = 8000
    debug: bool = True

    # Security settings
    secret_key: str = "your-secret-key-here"
    algorithm: str = "HS256"
    access_token_expire_minutes: int = 30

    # External services
    kafka_bootstrap_servers: str = "localhost:9092"
    sar_trigger_topic: str = "sar-trigger-events"

    class Config:
        env_file = ".env"
        case_sensitive = False

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        # If Supabase credentials are provided, construct the database URL
        if self.supabase_url and self.supabase_service_key:
            # Extract project ref from Supabase URL
            project_ref = self.supabase_url.replace("https://", "").replace("http://", "").split(".")[0]
            # For Supabase, the database URL follows this pattern
            # We'll need the password from the service key or ask user to provide it
            # For now, assume the database_url is set directly, or we can modify this
            pass

settings = Settings()