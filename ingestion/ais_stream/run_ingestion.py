import asyncio
import websockets
import json
import os
import logging
import signal
import sys
from dotenv import load_dotenv

# Add the project root to sys.path to ensure modules can be imported
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))

from ingestion.ais_stream.ais_ingestion import AISProducerWrapper
from ingestion.ais_stream.dead_letter.invalid_messages import DeadLetterHandler

# Load env vars
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

AIS_STREAM_URL = "wss://stream.aisstream.io/v0/stream"
API_KEY = os.getenv("aisstream_api_key")

if not API_KEY:
    logger.error("API Key not found. Please set aisstream_api_key in your .env file.")
    sys.exit(1)


def _resolve_bounding_boxes():
    """
    Resolve stream bounding boxes from AIS_STREAM_BOUNDING_BOXES if provided.

    Expected JSON format:
    [
      [[min_lat, min_lon], [max_lat, max_lon]],
      ...
    ]
    """
    raw = os.getenv("AIS_STREAM_BOUNDING_BOXES", "").strip()
    if not raw:
        return [[[-90, -180], [90, 180]]]

    try:
        boxes = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ValueError("AIS_STREAM_BOUNDING_BOXES must be valid JSON") from exc

    if not isinstance(boxes, list) or not boxes:
        raise ValueError("AIS_STREAM_BOUNDING_BOXES must be a non-empty JSON array")

    normalized = []
    for box in boxes:
        if not isinstance(box, list) or len(box) != 2:
            raise ValueError("Each bounding box must have two coordinate pairs")
        low, high = box
        if not isinstance(low, list) or not isinstance(high, list) or len(low) != 2 or len(high) != 2:
            raise ValueError("Bounding box coordinates must be [[min_lat, min_lon], [max_lat, max_lon]]")

        min_lat, min_lon = float(low[0]), float(low[1])
        max_lat, max_lon = float(high[0]), float(high[1])

        if min_lat > max_lat or min_lon > max_lon:
            raise ValueError("Bounding box min values must be <= max values")

        normalized.append([[min_lat, min_lon], [max_lat, max_lon]])

    return normalized

async def run_ingestion():
    """
    Main ingestion loop. Connects to WebSocket, validates, and routes messages.
    """
    producer = None
    dlq = None
    
    try:
        producer = AISProducerWrapper()
        dlq = DeadLetterHandler()
    except Exception as e:
        logger.critical(f"Failed to initialize Kafka components: {e}")
        return

    try:
        bounding_boxes = _resolve_bounding_boxes()
    except ValueError as exc:
        logger.critical("Invalid AIS_STREAM_BOUNDING_BOXES: %s", exc)
        if producer:
            producer.close()
        if dlq:
            dlq.close()
        return

    logger.info("Starting AIS Ingestion...")
    logger.info("Using %d stream bounding box(es)", len(bounding_boxes))

    async with websockets.connect(AIS_STREAM_URL) as websocket:
        subscription_message = {
            "APIKey": API_KEY,
            "BoundingBoxes": bounding_boxes,
            "FilterMessageTypes": ["PositionReport"]
        }
        
        await websocket.send(json.dumps(subscription_message))
        logger.info(f"Subscribed to {AIS_STREAM_URL}")

        try:
            async for message in websocket:
                try:
                    msg_json = json.loads(message)
                    
                    # Basic Validation
                    if "MessageType" not in msg_json:
                        dlq.handle_invalid_message(message, "Missing MessageType")
                        continue

                    msg_type = msg_json["MessageType"]
                    
                    if msg_type == "PositionReport":
                        # In a real scenario, we might use pydantic here for deeper validation
                        # For now, we assume the structure is mostly correct if it comes from aisstream
                        producer.publish_record(msg_json)
                    else:
                        logger.debug(f"Ignored message type: {msg_type}")

                except json.JSONDecodeError:
                    dlq.handle_invalid_message(message, "Invalid JSON")
                except Exception as e:
                    dlq.handle_invalid_message(message, f"Processing Error: {str(e)}")
                    
        except websockets.exceptions.ConnectionClosed as e:
            logger.warning(f"WebSocket connection closed: {e}")
        except Exception as e:
            logger.error(f"Unexpected error in ingestion loop: {e}")
        finally:
            if producer: producer.close()
            if dlq: dlq.close()

if __name__ == "__main__":
    try:
        asyncio.run(run_ingestion())
    except KeyboardInterrupt:
        logger.info("Ingestion stopped by user.")
