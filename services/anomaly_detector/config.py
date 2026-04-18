import os
from dataclasses import dataclass, field


def _env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


@dataclass
class AnomalyDetectorConfig:
    kafka_bootstrap_servers: str = field(default_factory=lambda: os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092"))
    kafka_group_id: str = field(default_factory=lambda: os.getenv("AIS_ANOMALY_GROUP_ID", "ais-anomaly-detector-v1"))
    input_topic: str = field(default_factory=lambda: os.getenv("AIS_FEATURES_TOPIC", "ais.features.vessel_tracks"))
    output_topic: str = field(default_factory=lambda: os.getenv("AIS_ANOMALIES_TOPIC", "ais.anomalies.events"))
    deadletter_topic: str = field(default_factory=lambda: os.getenv("AIS_DEADLETTER_TOPIC", "ais.deadletter"))

    model_name: str = field(default_factory=lambda: os.getenv("AIS_ANOMALY_MODEL_NAME", "IsolationForest-Placeholder-v0"))
    anomaly_score_threshold: float = field(default_factory=lambda: float(os.getenv("AIS_ANOMALY_SCORE_THRESHOLD", "0.65")))
    placeholder_force_emit: bool = field(default_factory=lambda: _env_bool("AIS_ANOMALY_PLACEHOLDER_FORCE_EMIT", False))
    placeholder_forced_score: float = field(default_factory=lambda: float(os.getenv("AIS_ANOMALY_PLACEHOLDER_SCORE", "0.95")))
    placeholder_anomaly_type: str = field(default_factory=lambda: os.getenv("AIS_ANOMALY_PLACEHOLDER_TYPE", "placeholder_detected"))

    poll_timeout_ms: int = field(default_factory=lambda: int(os.getenv("AIS_ANOMALY_POLL_TIMEOUT_MS", "1000")))
