import os
from dataclasses import dataclass


@dataclass
class StreamProcessorConfig:
    kafka_bootstrap_servers: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
    kafka_group_id: str = os.getenv("AIS_STREAM_PROCESSOR_GROUP_ID", "ais-stream-processor-v1")
    input_topic: str = os.getenv("AIS_RAW_TOPIC", "ais.raw.position_reports")
    cleaned_topic: str = os.getenv("AIS_CLEANED_TOPIC", "ais.cleaned.position_reports")
    features_topic: str = os.getenv("AIS_FEATURES_TOPIC", "ais.features.vessel_tracks")
    deadletter_topic: str = os.getenv("AIS_DEADLETTER_TOPIC", "ais.deadletter")

    state_backend: str = os.getenv("AIS_STATE_BACKEND", "redis")
    redis_url: str = os.getenv("AIS_REDIS_URL", "redis://localhost:6379/0")
    vessel_window_size: int = int(os.getenv("AIS_VESSEL_WINDOW_SIZE", "20"))
    vessel_state_ttl_sec: int = int(os.getenv("AIS_VESSEL_STATE_TTL_SEC", "86400"))

    poll_timeout_ms: int = int(os.getenv("AIS_POLL_TIMEOUT_MS", "1000"))
