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
    encoder_checkpoint_path: str = field(
        default_factory=lambda: os.getenv(
            "AIS_ENCODER_CHECKPOINT_PATH",
            "preprocessing/outputs/ais_model/encoder.pt",
        )
    )
    memory_dir: str = field(default_factory=lambda: os.getenv("AIS_MEMORY_DIR", "preprocessing/outputs/ais_memory_bank"))
    realtime_min_window_points: int = field(default_factory=lambda: int(os.getenv("AIS_REALTIME_MIN_WINDOW_POINTS", "4")))
    realtime_k_neighbors: int = field(default_factory=lambda: int(os.getenv("AIS_REALTIME_K_NEIGHBORS", "5")))
    realtime_use_faiss: bool = field(default_factory=lambda: os.getenv("AIS_REALTIME_USE_FAISS", "true").lower() in {"1", "true", "yes", "on"})

    poll_timeout_ms: int = field(default_factory=lambda: int(os.getenv("AIS_ANOMALY_POLL_TIMEOUT_MS", "1000")))
