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

if not API_KEY:
    logger.error("API Key not found. Please set aisstream_api_key in your .env file.")
    exit(1)

async def connect_ais_stream():
    """
    Connects to the AISStream WebSocket and prints received messages.
    """
    async with websockets.connect(AIS_STREAM_URL) as websocket:
        subscription_message = {
            "APIKey": API_KEY,
            "BoundingBoxes": [[[-90, -180], [90, 180]]],  # Global Coverage (India focus intent but global requested in bounding box)
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
                        # In the future, this will be pushed to Kafka
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

if __name__ == "__main__":
    try:
        asyncio.run(connect_ais_stream())
    except KeyboardInterrupt:
        logger.info("Stopped by user.")
