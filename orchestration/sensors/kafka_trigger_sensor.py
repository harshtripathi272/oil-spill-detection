"""Kafka trigger sensor for SAR event-driven DAG starts."""

import json
import logging
from typing import Any, Dict, Optional

from airflow.exceptions import AirflowException
from airflow.sensors.base import BaseSensorOperator
from kafka import KafkaConsumer


class KafkaTriggerSensor(BaseSensorOperator):
    """
    Poll a Kafka topic until one trigger event is consumed.

    The consumed event is returned via XCom and can be used downstream as DAG input.
    """

    template_fields = ("topic", "bootstrap_servers", "group_id")

    def __init__(
        self,
        topic: str,
        bootstrap_servers: str,
        group_id: str,
        poll_timeout_ms: int = 1000,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.topic = topic
        self.bootstrap_servers = bootstrap_servers
        self.group_id = group_id
        self.poll_timeout_ms = poll_timeout_ms
        self._last_event: Optional[Dict[str, Any]] = None

    def poke(self, context):
        del context
        consumer = None

        try:
            consumer = KafkaConsumer(
                self.topic,
                bootstrap_servers=self.bootstrap_servers,
                group_id=self.group_id,
                enable_auto_commit=False,
                auto_offset_reset="earliest",
                value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                consumer_timeout_ms=self.poll_timeout_ms,
            )

            records = consumer.poll(timeout_ms=self.poll_timeout_ms, max_records=1)
            if not records:
                logging.info("No events in Kafka topic %s yet", self.topic)
                return False

            for _, partition_records in records.items():
                for record in partition_records:
                    payload = record.value
                    if not isinstance(payload, dict):
                        raise AirflowException("Kafka trigger payload must be a JSON object")

                    self._last_event = payload
                    consumer.commit()
                    logging.info("Consumed trigger event from %s", self.topic)
                    return True

            return False

        except Exception as exc:
            raise AirflowException(f"Kafka trigger sensor failed: {exc}") from exc
        finally:
            if consumer:
                consumer.close()

    def execute(self, context):
        super().execute(context)
        if not self._last_event:
            raise AirflowException("Kafka trigger sensor completed without event payload")
        return self._last_event
