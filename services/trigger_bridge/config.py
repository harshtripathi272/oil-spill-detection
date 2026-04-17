import os
from dataclasses import dataclass, field


@dataclass
class TriggerBridgeConfig:
    kafka_bootstrap_servers: str = field(default_factory=lambda: os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092"))
    kafka_group_id: str = field(default_factory=lambda: os.getenv("AIS_TRIGGER_BRIDGE_GROUP_ID", "ais-trigger-bridge-v1"))
    input_topic: str = field(default_factory=lambda: os.getenv("AIS_ANOMALIES_TOPIC", "ais.anomalies.events"))
    output_topic: str = field(default_factory=lambda: os.getenv("SAR_TRIGGER_TOPIC", "sar.trigger.events"))
    deadletter_topic: str = field(default_factory=lambda: os.getenv("AIS_DEADLETTER_TOPIC", "ais.deadletter"))

    trigger_score_threshold: float = field(default_factory=lambda: float(os.getenv("SAR_TRIGGER_SCORE_THRESHOLD", "0.75")))
    allowed_bbox: str = field(default_factory=lambda: os.getenv("SAR_TRIGGER_ALLOWED_BBOX", ""))

    poll_timeout_ms: int = field(default_factory=lambda: int(os.getenv("AIS_TRIGGER_BRIDGE_POLL_TIMEOUT_MS", "1000")))
