import os
from dataclasses import dataclass


@dataclass
class AnomalyDetectorConfig:
    kafka_bootstrap_servers: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
    kafka_group_id: str = os.getenv("AIS_ANOMALY_GROUP_ID", "ais-anomaly-detector-v1")
    input_topic: str = os.getenv("AIS_FEATURES_TOPIC", "ais.features.vessel_tracks")
    output_topic: str = os.getenv("AIS_ANOMALIES_TOPIC", "ais.anomalies.events")
    deadletter_topic: str = os.getenv("AIS_DEADLETTER_TOPIC", "ais.deadletter")

    model_name: str = os.getenv("AIS_ANOMALY_MODEL_NAME", "IsolationForest-Placeholder-v0")
    anomaly_score_threshold: float = float(os.getenv("AIS_ANOMALY_SCORE_THRESHOLD", "0.65"))

    poll_timeout_ms: int = int(os.getenv("AIS_ANOMALY_POLL_TIMEOUT_MS", "1000"))
