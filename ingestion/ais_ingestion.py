import asyncio
import websockets
import json
import os
import logging
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

AIS_STREAM_URL = "wss://stream.aisstream.io/v0/stream"
API_KEY = os.getenv("aisstream_api_key")
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
KAFKA_TOPIC = "ais_position_reports"

if not API_KEY:
    logger.error("API Key not found. Please set aisstream_api_key in your .env file.")
    exit(1)

from kafka import KafkaProducer, errors as kafka_errors

def get_kafka_producer():
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            retries=5
        )
        logger.info(f"Connected to Kafka at {KAFKA_BOOTSTRAP_SERVERS}")
        return producer
    except kafka_errors.NoBrokersAvailable:
        logger.error(f"No Kafka brokers available at {KAFKA_BOOTSTRAP_SERVERS}")
        return None
    except Exception as e:
        logger.error(f"Failed to create Kafka producer: {e}")
        return None

async def connect_ais_stream():
    """
    Connects to the AISStream WebSocket and pushes PositionReports to Kafka.
    """
    producer = get_kafka_producer()
    
    async with websockets.connect(AIS_STREAM_URL) as websocket:
        subscription_message = {
            "APIKey": API_KEY,
            "BoundingBoxes": [[[-90, -180], [90, 180]]],  # Global Coverage
            # "BoundingBoxes": [[[5.0, 68.0], [37.0, 97.0]]],
            "FilterMessageTypes": ["PositionReport"]
        }

        subscribe_json = json.dumps(subscription_message)
        logger.info(f"Connecting to {AIS_STREAM_URL}...")
        await websocket.send(subscribe_json)
        logger.info("Subscription sent. Waiting for messages...")

        try:
            async for message in websocket:
                try:
                    msg_json = json.loads(message)
                    message_type = msg_json.get("MessageType")
                    
                    if message_type == "PositionReport":
                        # Push raw message or specific fields to Kafka
                        if producer:
                            try:
                                future = producer.send(KAFKA_TOPIC, value=msg_json)
                                # future.get(timeout=10) # Blocking call, probably bad for async loop, rely on async callbacks or fire-and-forget with logging
                                logger.info(f"Sent PositionReport to Kafka topic '{KAFKA_TOPIC}'")
                            except Exception as k_e:
                                logger.error(f"Failed to send to Kafka: {k_e}")
                        
                        position_report = msg_json.get("Message", {}).get("PositionReport", {})
                        mmsi = msg_json.get("MetaData", {}).get("MMSI")
                        lat = position_report.get("Latitude")
                        lon = position_report.get("Longitude")
                        logger.info(f"Received PositionReport - MMSI: {mmsi}, Lat: {lat}, Lon: {lon}")
                    else:
                        logger.debug(f"Received other message type: {message_type}")
                        
                except json.JSONDecodeError:
                    logger.error("Failed to decode JSON message")
                except Exception as e:
                    logger.error(f"Error processing message: {e}")

        except websockets.exceptions.ConnectionClosed as e:
            logger.warning(f"Connection closed: {e}")
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
        finally:
            if producer:
                producer.close()

if __name__ == "__main__":
    try:
        asyncio.run(connect_ais_stream())
    except KeyboardInterrupt:
        logger.info("Stopped by user.")
