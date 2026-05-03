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
from services.anomaly_detector.config import AnomalyDetectorConfig
from services.anomaly_detector.model import AISRealtimeMemoryBankModel, build_anomaly_event

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)
# Suppress noisy Kafka connection logs 
logging.getLogger("kafka").setLevel(logging.ERROR)

SHUTDOWN = False


def _handle_signal(signum, frame):
    del signum, frame
    global SHUTDOWN
    SHUTDOWN = True
    logger.info("Shutdown signal received. Stopping anomaly detector...")


def _build_consumer(cfg: AnomalyDetectorConfig) -> KafkaConsumer:
    return KafkaConsumer(
        cfg.input_topic,
        bootstrap_servers=cfg.kafka_bootstrap_servers,
        group_id=cfg.kafka_group_id,
        enable_auto_commit=True,
        auto_offset_reset="latest",
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        consumer_timeout_ms=cfg.poll_timeout_ms,
    )


def _build_producer(cfg: AnomalyDetectorConfig) -> KafkaProducer:
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
    cfg = AnomalyDetectorConfig()
    model = AISRealtimeMemoryBankModel(
        model_name=cfg.model_name,
        checkpoint_path=cfg.encoder_checkpoint_path,
        memory_dir=cfg.memory_dir,
        score_threshold=cfg.anomaly_score_threshold,
        k_neighbors=cfg.realtime_k_neighbors,
        min_window_points=cfg.realtime_min_window_points,
        trajectory_window_size=cfg.realtime_trajectory_window_size,
        score_smoothing_window_size=cfg.realtime_score_smoothing_window_size,
        use_faiss=cfg.realtime_use_faiss,
    )

    logger.info("Starting anomaly detector with model=%s", cfg.model_name)
    logger.info("Using realtime checkpoint: %s", cfg.encoder_checkpoint_path)
    logger.info("Using realtime memory bank: %s", cfg.memory_dir)
    logger.info("Kafka bootstrap=%s input=%s output=%s", cfg.kafka_bootstrap_servers, cfg.input_topic, cfg.output_topic)

    consumer = None
    producer = None
    dlq = None

    processed = 0
    emitted = 0
    discarded = 0
    invalid = 0

    try:
        consumer = _build_consumer(cfg)
        producer = _build_producer(cfg)
        dlq = DeadLetterHandler(
            bootstrap_servers=cfg.kafka_bootstrap_servers,
            dlq_topic=cfg.deadletter_topic,
        )
    except Exception as exc:
        logger.critical("Failed to initialize anomaly detector clients: %s", exc)
        return

    while not SHUTDOWN:
        try:
            for message in consumer:
                if SHUTDOWN:
                    break

                processed += 1
                feature_event = message.value

                model_score = model.infer(feature_event)
                anomaly_event, err = build_anomaly_event(
                    model_name=cfg.model_name,
                    feature_event=feature_event,
                    model_score=model_score,
                )

                if err:
                    invalid += 1
                    dlq.handle_invalid_message(
                        raw_message=json.dumps(feature_event),
                        error_reason=f"anomaly_detector_validation: {err}",
                    )
                    continue

                if model_score.score < cfg.anomaly_score_threshold:
                    discarded += 1
                    continue

                logger.info("🚨 [ANOMALY DETECTED] Vessel: %s, Score: %.4f, Lat: %.4f, Lon: %.4f", 
                            anomaly_event['vessel_id'], anomaly_event['score'], anomaly_event['lat'], anomaly_event['lon'])
                _publish(producer, cfg.output_topic, anomaly_event)
                emitted += 1

                if processed % 100 == 0:
                    logger.info(
                        "processed=%d emitted=%d discarded=%d invalid=%d",
                        processed,
                        emitted,
                        discarded,
                        invalid,
                    )

        except Exception as exc:
            logger.exception("Anomaly detection loop error: %s", exc)

    if consumer:
        consumer.close()
    if producer:
        producer.flush()
        producer.close()
    if dlq:
        dlq.close()

    logger.info(
        "Stopped anomaly detector. processed=%d emitted=%d discarded=%d invalid=%d",
        processed,
        emitted,
        discarded,
        invalid,
    )


if __name__ == "__main__":
    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)
    run()
