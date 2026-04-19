import os
from dataclasses import dataclass, field


@dataclass
class AnomalyDetectorConfig:
    kafka_bootstrap_servers: str = field(default_factory=lambda: os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092"))
    kafka_group_id: str = field(default_factory=lambda: os.getenv("AIS_ANOMALY_GROUP_ID", "ais-anomaly-detector-v1"))
    input_topic: str = field(default_factory=lambda: os.getenv("AIS_FEATURES_TOPIC", "ais.features.vessel_tracks"))
    output_topic: str = field(default_factory=lambda: os.getenv("AIS_ANOMALIES_TOPIC", "ais.anomalies.events"))
    deadletter_topic: str = field(default_factory=lambda: os.getenv("AIS_DEADLETTER_TOPIC", "ais.deadletter"))

    model_name: str = field(default_factory=lambda: os.getenv("AIS_ANOMALY_MODEL_NAME", "AIS-Contrastive-Encoder-v1"))
    anomaly_score_threshold: float = field(default_factory=lambda: float(os.getenv("AIS_ANOMALY_SCORE_THRESHOLD", "0.8")))
    ais_inference_scores_path: str = field(
        default_factory=lambda: os.getenv(
            "AIS_INFERENCE_SCORES_PATH",
            "preprocessing/outputs/ais_inference/anomaly_scores.parquet",
        )
    )
    ais_score_match_window_sec: int = field(default_factory=lambda: int(os.getenv("AIS_SCORE_MATCH_WINDOW_SEC", "21600")))
    ais_scores_reload_sec: int = field(default_factory=lambda: int(os.getenv("AIS_SCORES_RELOAD_SEC", "300")))

    poll_timeout_ms: int = field(default_factory=lambda: int(os.getenv("AIS_ANOMALY_POLL_TIMEOUT_MS", "1000")))
