import json
import logging
import os
from kafka import KafkaProducer
from kafka.errors import KafkaError

logger = logging.getLogger(__name__)

class AISProducerWrapper:
    """
    Wraps a Kafka Consumer/Producer to ingest valid AIS messages.
    """
    def __init__(self, bootstrap_servers: str = None, topic: str = "ais.raw.position_reports"):
        """
        Initialize the AISProducerWrapper.

        Args:
            bootstrap_servers (str): Kafka bootstrap servers.
            topic (str): The Kafka topic to produce valid messages to.
        """
        self.topic = topic
        self.bootstrap_servers = bootstrap_servers or os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
        self.producer = None

        try:
            self.producer = KafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                retries=5,
                acks='all' # Ensure durability
            )
            logger.info(f"AISProducerWrapper initialized for topic '{self.topic}' at {self.bootstrap_servers}")
        except Exception as e:
            logger.error(f"Failed to initialize AISProducerWrapper producer: {e}")
            raise e

    def publish_record(self, record: dict):
        """
        Publishes a validated record to Kafka.

        Args:
            record (dict): The validated AIS message dictionary.
        """
        if not self.producer:
            logger.error("Producer not initialized. Cannot send message.")
            return

        try:
            # Fire and forget with logging (async)
            future = self.producer.send(self.topic, value=record)
            future.add_callback(self._on_success)
            future.add_errback(self._on_error)
        except KafkaError as e:
            logger.error(f"Failed to schedule message for sending: {e}")

    def _on_success(self, record_metadata):
        # logging every message might be too verbose for high throughput, keeping it debug
        logger.debug(f"Message sent to {record_metadata.topic} partition {record_metadata.partition} offset {record_metadata.offset}")

    def _on_error(self, exc):
        logger.error(f"Message delivery failed: {exc}")

    def close(self):
        """Closes the Kafka producer."""
        if self.producer:
            self.producer.flush()
            self.producer.close()
            logger.info("AISProducerWrapper producer closed.")
