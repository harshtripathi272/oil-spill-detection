import json
import logging
import os
import signal
import sys
from typing import Any, Dict

import requests
from dotenv import load_dotenv
from kafka import KafkaConsumer, KafkaProducer

# Add the project root to sys.path to ensure modules can be imported
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../")))

from ingestion.ais_stream.dead_letter.invalid_messages import DeadLetterHandler
from services.trigger_bridge.config import TriggerBridgeConfig
from services.trigger_bridge.filtering import build_trigger_event, should_forward_event

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
    logger.info("Shutdown signal received. Stopping trigger bridge...")


def _build_consumer(cfg: TriggerBridgeConfig) -> KafkaConsumer:
    return KafkaConsumer(
        cfg.input_topic,
        bootstrap_servers=cfg.kafka_bootstrap_servers,
        group_id=cfg.kafka_group_id,
        enable_auto_commit=True,
        auto_offset_reset="latest",
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        consumer_timeout_ms=cfg.poll_timeout_ms,
    )


def _build_producer(cfg: TriggerBridgeConfig) -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=cfg.kafka_bootstrap_servers,
        value_serializer=lambda payload: json.dumps(payload).encode("utf-8"),
        retries=5,
        acks="all",
    )


def _publish(producer: KafkaProducer, topic: str, payload: Dict[str, Any]) -> None:
    future = producer.send(topic, payload)
    future.get(timeout=10)


def _trigger_airflow_dag(cfg: TriggerBridgeConfig, trigger_event: Dict[str, Any]) -> bool:
    """Trigger the Airflow DAG with the trigger event as configuration.
    
    Args:
        cfg: TriggerBridgeConfig instance
        trigger_event: The trigger event to pass as DAG run configuration
        
    Returns:
        True if trigger succeeded or is disabled, False if trigger failed
    """
    if not cfg.airflow_trigger_enabled:
        return True
    
    try:
        url = f"{cfg.airflow_api_base_url}/dags/{cfg.airflow_dag_id}/dagRuns"
        headers = {"Content-Type": "application/json"}
        payload = {"conf": trigger_event}
        
        response = requests.post(
            url,
            json=payload,
            headers=headers,
            auth=(cfg.airflow_username, cfg.airflow_password),
            timeout=10,
        )
        
        if response.status_code in (200, 201):
            logger.info("✅ [AIRFLOW TRIGGERED] DAG run created for event: %s", trigger_event.get("incident_id"))
            return True
        else:
            logger.warning(
                "⚠️ [AIRFLOW TRIGGER FAILED] Status: %d, Response: %s",
                response.status_code,
                response.text[:200],
            )
            return False
            
    except requests.exceptions.Timeout:
        logger.warning("⚠️ [AIRFLOW TRIGGER TIMEOUT] Could not reach Airflow API at %s", cfg.airflow_api_base_url)
        return False
    except requests.exceptions.ConnectionError:
        logger.warning("⚠️ [AIRFLOW TRIGGER ERROR] Connection failed to %s", cfg.airflow_api_base_url)
        return False
    except Exception as exc:
        logger.warning("⚠️ [AIRFLOW TRIGGER ERROR] %s", exc)
        return False


def run() -> None:
    cfg = TriggerBridgeConfig()

    logger.info("Starting trigger bridge")
    logger.info("Kafka bootstrap=%s input=%s output=%s", cfg.kafka_bootstrap_servers, cfg.input_topic, cfg.output_topic)
    logger.info(
        "Trigger filter config: score_threshold=%s allowed_bbox=%s",
        cfg.trigger_score_threshold,
        cfg.allowed_bbox or "<none>",
    )

    consumer = None
    producer = None
    dlq = None

    processed = 0
    forwarded = 0
    filtered = 0
    invalid = 0

    vessel_cooldowns: Dict[str, float] = {}
    COOLDOWN_SEC = 2 * 3600  # 2 hours

    try:

        consumer = _build_consumer(cfg)
        producer = _build_producer(cfg)
        dlq = DeadLetterHandler(
            bootstrap_servers=cfg.kafka_bootstrap_servers,
            dlq_topic=cfg.deadletter_topic,
        )
    except Exception as exc:
        logger.critical("Failed to initialize trigger bridge clients: %s", exc)
        return

    while not SHUTDOWN:
        try:
            for message in consumer:
                if SHUTDOWN:
                    break

                processed += 1
                anomaly_event = message.value

                should_forward, reason = should_forward_event(
                    anomaly_event=anomaly_event,
                    threshold=cfg.trigger_score_threshold,
                    allowed_bbox=cfg.allowed_bbox,
                )
                if not should_forward:
                    filtered += 1
                    if filtered <= 5:
                        logger.info("Filtered anomaly event: %s", reason)
                    continue

                try:

                    import time
                    current_time = time.time()
                    vessel_id = str(anomaly_event.get("vessel_id", ""))
                    
                    if processed % 100 == 0:
                        vessel_cooldowns = {vid: ts for vid, ts in vessel_cooldowns.items() if current_time - ts < COOLDOWN_SEC}

                    if vessel_id and vessel_id in vessel_cooldowns and (current_time - vessel_cooldowns[vessel_id] < COOLDOWN_SEC):
                        filtered += 1
                        continue
                    if vessel_id:
                        vessel_cooldowns[vessel_id] = current_time
                except Exception:
                    pass

                trigger_event, err = build_trigger_event(anomaly_event)
                if err:
                    invalid += 1
                    dlq.handle_invalid_message(
                        raw_message=json.dumps(anomaly_event),
                        error_reason=f"trigger_bridge_validation: {err}",
                    )
                    continue


                _publish(producer, cfg.output_topic, trigger_event)
                _trigger_airflow_dag(cfg, trigger_event)
                forwarded += 1
                logger.info("🚀 [SAR TRIGGER SENT] Vessel: %s, Score: %.4f, Lat: %.4f, Lon: %.4f", 
                            vessel_id, anomaly_event.get('score', 0), anomaly_event.get('lat', 0), anomaly_event.get('lon', 0))

                if processed % 100 == 0:
                    logger.info(
                        "processed=%d forwarded=%d filtered=%d invalid=%d",
                        processed,
                        forwarded,
                        filtered,
                        invalid,
                    )

        except Exception as exc:
            logger.exception("Trigger bridge loop error: %s", exc)

    if consumer:
        consumer.close()
    if producer:
        producer.flush()
        producer.close()
    if dlq:
        dlq.close()

    logger.info(
        "Stopped trigger bridge. processed=%d forwarded=%d filtered=%d invalid=%d",
        processed,
        forwarded,
        filtered,
        invalid,
    )


if __name__ == "__main__":
    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)
    run()
