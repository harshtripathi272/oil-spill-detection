import argparse
import json
import logging
import os
import sys
from typing import Iterable, List

from dotenv import load_dotenv

# Add the project root to sys.path to ensure modules can be imported
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))

from ingestion.ais_stream.ais_ingestion import AISProducerWrapper
from ingestion.ais_stream.dead_letter.invalid_messages import DeadLetterHandler

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def _load_messages(file_path: str) -> Iterable[str]:
    """
    Load messages from a JSON file.

    Supported formats:
    - NDJSON: one JSON object per line
    - JSON array: [ {...}, {...} ]
    - Single JSON object
    """
    with open(file_path, 'r', encoding='utf-8') as f:
        content = f.read().strip()

    if not content:
        return []

    # First try to parse full JSON payload (array/object).
    try:
        payload = json.loads(content)
        if isinstance(payload, list):
            return [json.dumps(item) for item in payload]
        if isinstance(payload, dict):
            return [json.dumps(payload)]
    except json.JSONDecodeError:
        pass

    # Fallback: treat as NDJSON.
    return [line for line in content.splitlines() if line.strip()]


def _is_position_report(message: dict) -> bool:
    message_type = message.get('MessageType')
    if isinstance(message_type, str):
        return message_type == 'PositionReport'
    if isinstance(message_type, dict):
        return 'PositionReport' in message_type
    return False


def ingest_file(file_path: str, max_messages: int = 0) -> None:
    producer = None
    dlq = None

    try:
        producer = AISProducerWrapper()
        dlq = DeadLetterHandler()
    except Exception as exc:
        logger.critical(f'Failed to initialize Kafka components: {exc}')
        return

    logger.info(f'Starting file ingestion from: {file_path}')
    processed = 0
    published = 0
    invalid = 0

    try:
        raw_messages = _load_messages(file_path)

        for raw in raw_messages:
            if max_messages > 0 and processed >= max_messages:
                logger.info(f'Reached max_messages={max_messages}, stopping early.')
                break

            processed += 1
            try:
                msg_json = json.loads(raw)

                if not isinstance(msg_json, dict):
                    invalid += 1
                    dlq.handle_invalid_message(raw, 'Message must be a JSON object')
                    continue

                if not _is_position_report(msg_json):
                    invalid += 1
                    dlq.handle_invalid_message(raw, 'Not a PositionReport message')
                    continue

                producer.publish_record(msg_json)
                published += 1
            except json.JSONDecodeError:
                invalid += 1
                dlq.handle_invalid_message(raw, 'Invalid JSON')
            except Exception as exc:
                invalid += 1
                dlq.handle_invalid_message(raw, f'Processing Error: {exc}')

        logger.info(
            f'File ingestion completed. processed={processed}, published={published}, invalid={invalid}'
        )
    finally:
        if producer:
            producer.close()
        if dlq:
            dlq.close()


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='Replay AIS messages from file into Kafka.')
    parser.add_argument('--input', required=True, help='Path to NDJSON/JSON file containing AIS messages')
    parser.add_argument(
        '--max-messages',
        type=int,
        default=0,
        help='Optional max messages to ingest. 0 means all messages.'
    )
    return parser.parse_args()


if __name__ == '__main__':
    args = _parse_args()
    ingest_file(file_path=args.input, max_messages=args.max_messages)
