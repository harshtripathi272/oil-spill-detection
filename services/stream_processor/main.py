import json
import logging
import os
import signal
import sys
from typing import Any, Dict

from dotenv import load_dotenv
from kafka import KafkaConsumer, KafkaProducer

# Add the project root to sys.path to ensure modules can be imported
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../")))

from ingestion.ais_stream.dead_letter.invalid_messages import DeadLetterHandler
from services.stream_processor.config import StreamProcessorConfig
from services.stream_processor.processing import build_feature_event, validate_and_normalize
from services.stream_processor.vessel_state import VesselStateManager

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

SHUTDOWN = False


def _handle_signal(signum, frame):
    del signum, frame
    global SHUTDOWN
    SHUTDOWN = True
    logger.info("Shutdown signal received. Stopping stream processor...")


def _build_consumer(cfg: StreamProcessorConfig) -> KafkaConsumer:
    return KafkaConsumer(
        cfg.input_topic,
        bootstrap_servers=cfg.kafka_bootstrap_servers,
        group_id=cfg.kafka_group_id,
        enable_auto_commit=True,
        auto_offset_reset="latest",
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        consumer_timeout_ms=cfg.poll_timeout_ms,
    )


def _build_producer(cfg: StreamProcessorConfig) -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=cfg.kafka_bootstrap_servers,
        value_serializer=lambda payload: json.dumps(payload).encode("utf-8"),
        retries=5,
        acks="all",
    )


def _publish(producer: KafkaProducer, topic: str, payload: Dict[str, Any]) -> None:
    future = producer.send(topic, payload)
    future.get(timeout=10)


def run() -> None:
    cfg = StreamProcessorConfig()

    logger.info("Starting stream processor")
    logger.info(
        "Kafka bootstrap=%s input=%s cleaned=%s features=%s",
        cfg.kafka_bootstrap_servers,
        cfg.input_topic,
        cfg.cleaned_topic,
        cfg.features_topic,
    )

    consumer = None
    producer = None
    dlq = None

    state = VesselStateManager(
        backend=cfg.state_backend,
        redis_url=cfg.redis_url,
        window_size=cfg.vessel_window_size,
        ttl_sec=cfg.vessel_state_ttl_sec,
    )

    processed = 0
    cleaned_count = 0
    feature_count = 0
    invalid_count = 0

    try:
        consumer = _build_consumer(cfg)
        producer = _build_producer(cfg)
        dlq = DeadLetterHandler(
            bootstrap_servers=cfg.kafka_bootstrap_servers,
            dlq_topic=cfg.deadletter_topic,
        )
    except Exception as exc:
        logger.critical("Failed to initialize stream processor clients: %s", exc)
        return

    while not SHUTDOWN:
        try:
            for message in consumer:
                if SHUTDOWN:
                    break

                processed += 1
                raw_message = message.value

                cleaned, err = validate_and_normalize(raw_message)
                if err:
                    invalid_count += 1
                    dlq.handle_invalid_message(
                        raw_message=json.dumps(raw_message),
                        error_reason=f"stream_processor_validation: {err}",
                    )
                    continue

                _publish(producer, cfg.cleaned_topic, cleaned)
                cleaned_count += 1

                vessel_id = cleaned["vessel_id"]
                vessel_state = state.get_state(vessel_id)
                prev_ts = None
                if vessel_state.get("timestamps"):
                    prev_ts = vessel_state["timestamps"][-1]

                current_ts = cleaned["timestamp"]
                reset_voyage = False
                if prev_ts:
                    try:
                        from datetime import datetime

                        prev_dt = datetime.fromisoformat(str(prev_ts).replace("Z", "+00:00"))
                        curr_dt = datetime.fromisoformat(str(current_ts).replace("Z", "+00:00"))
                        gap_hours = max((curr_dt - prev_dt).total_seconds(), 0.0) / 3600.0
                        reset_voyage = gap_hours > cfg.voyage_gap_hours
                    except Exception:
                        reset_voyage = False

                if reset_voyage:
                    for key in vessel_state:
                        if hasattr(vessel_state[key], "clear"):
                            vessel_state[key].clear()

                feature_event = build_feature_event(cleaned, vessel_state)
                state.put_state(vessel_id, vessel_state)

                _publish(producer, cfg.features_topic, feature_event)
                feature_count += 1

                if processed % 100 == 0:
                    logger.info(
                        "processed=%d cleaned=%d features=%d invalid=%d",
                        processed,
                        cleaned_count,
                        feature_count,
                        invalid_count,
                    )

        except Exception as exc:
            logger.exception("Stream processing loop error: %s", exc)

    if consumer:
        consumer.close()
    if producer:
        producer.flush()
        producer.close()
    if dlq:
        dlq.close()

    logger.info(
        "Stopped stream processor. processed=%d cleaned=%d features=%d invalid=%d",
        processed,
        cleaned_count,
        feature_count,
        invalid_count,
    )


if __name__ == "__main__":
    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)
    run()
