import os
from dataclasses import dataclass


@dataclass
class TriggerBridgeConfig:
    kafka_bootstrap_servers: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
    kafka_group_id: str = os.getenv("AIS_TRIGGER_BRIDGE_GROUP_ID", "ais-trigger-bridge-v1")
    input_topic: str = os.getenv("AIS_ANOMALIES_TOPIC", "ais.anomalies.events")
    output_topic: str = os.getenv("SAR_TRIGGER_TOPIC", "sar.trigger.events")
    deadletter_topic: str = os.getenv("AIS_DEADLETTER_TOPIC", "ais.deadletter")

    trigger_score_threshold: float = float(os.getenv("SAR_TRIGGER_SCORE_THRESHOLD", "0.75"))
    allowed_bbox: str = os.getenv("SAR_TRIGGER_ALLOWED_BBOX", "")

    poll_timeout_ms: int = int(os.getenv("AIS_TRIGGER_BRIDGE_POLL_TIMEOUT_MS", "1000"))
