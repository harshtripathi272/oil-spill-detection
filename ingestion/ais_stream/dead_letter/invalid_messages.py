import json
import logging
import os
import time
from datetime import datetime
from kafka import KafkaProducer
from kafka.errors import KafkaError

logger = logging.getLogger(__name__)

class DeadLetterHandler:
    """
    Handles invalid or unparseable messages by publishing them to a dead-letter Kafka topic.
    """
    def __init__(self, bootstrap_servers: str = None, dlq_topic: str = "ais.deadletter"):
        """
        Initialize the DeadLetterHandler.

        Args:
            bootstrap_servers (str): Kafka bootstrap servers.
            dlq_topic (str): The Kafka topic to send dead-letter messages to.
        """
        self.dlq_topic = dlq_topic
        self.bootstrap_servers = bootstrap_servers or os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
        self.producer = None
        
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                retries=3
            )
            logger.info(f"DeadLetterHandler initialized for topic '{self.dlq_topic}' at {self.bootstrap_servers}")
        except Exception as e:
            logger.error(f"Failed to initialize DeadLetterHandler producer: {e}")

    def handle_invalid_message(self, raw_message: str, error_reason: str, metadata: dict = None):
        """
        Publishes an invalid message to the DLQ.

        Args:
            raw_message (str): The original raw message that failed processing.
            error_reason (str): Description of why the message is invalid.
            metadata (dict, optional): Additional context about the failure.
        """
        if not self.producer:
            logger.error("DeadLetterHandler producer is not active. Message dropped.")
            return

        payload = {
            "original_message": raw_message,
            "error_reason": error_reason,
            "failed_at": datetime.utcnow().isoformat(),
            "metadata": metadata or {}
        }

        try:
            self.producer.send(self.dlq_topic, value=payload)
            logger.warning(f"Message sent to DLQ '{self.dlq_topic}': {error_reason}")
        except KafkaError as e:
            logger.error(f"Failed to send message to DLQ: {e}")
        except Exception as e:
            logger.error(f"Unexpected error in DeadLetterHandler: {e}")

    def close(self):
        """Closes the Kafka producer."""
        if self.producer:
            self.producer.close()
            logger.info("DeadLetterHandler producer closed.")
