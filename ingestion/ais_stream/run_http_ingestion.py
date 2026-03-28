import argparse
import json
import logging
import os
import signal
import sys
import time
from typing import Any, Dict, Iterable, List, Optional, Tuple
from urllib import error, request

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

_SHUTDOWN = False


def _handle_signal(signum, frame):
    global _SHUTDOWN
    _SHUTDOWN = True
    logger.info('Shutdown signal received. Stopping HTTP ingestion loop...')


def _build_headers() -> Dict[str, str]:
    headers = {
        'Accept': 'application/json',
        'User-Agent': 'vesselwatch-http-ingestion/1.0',
    }

    api_key = os.getenv('AIS_HTTP_API_KEY', '').strip()
    auth_header = os.getenv('AIS_HTTP_AUTH_HEADER', 'Authorization').strip()
    auth_scheme = os.getenv('AIS_HTTP_AUTH_SCHEME', 'Bearer').strip()

    if api_key:
        if auth_scheme:
            headers[auth_header] = f'{auth_scheme} {api_key}'
        else:
            headers[auth_header] = api_key

    return headers


def _fetch_json(url: str, timeout_sec: int, retry_attempts: int, retry_backoff_sec: int) -> Any:
    headers = _build_headers()
    last_error: Optional[Exception] = None

    for attempt in range(1, retry_attempts + 1):
        try:
            req = request.Request(url=url, headers=headers, method='GET')
            with request.urlopen(req, timeout=timeout_sec) as resp:
                body = resp.read().decode('utf-8')
            return json.loads(body)
        except (error.HTTPError, error.URLError, TimeoutError, json.JSONDecodeError) as exc:
            last_error = exc
            logger.warning(f'HTTP fetch attempt {attempt}/{retry_attempts} failed: {exc}')
            if attempt < retry_attempts:
                time.sleep(retry_backoff_sec)

    raise RuntimeError(f'All HTTP fetch attempts failed. Last error: {last_error}')


def _extract_records(payload: Any) -> List[dict]:
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]

    if isinstance(payload, dict):
        for key in ('messages', 'data', 'results', 'items', 'records'):
            value = payload.get(key)
            if isinstance(value, list):
                return [item for item in value if isinstance(item, dict)]
        return [payload]

    return []


def _is_position_report_aisstream_shape(msg: dict) -> bool:
    msg_type = msg.get('MessageType')
    if isinstance(msg_type, str):
        return msg_type == 'PositionReport'
    if isinstance(msg_type, dict):
        return 'PositionReport' in msg_type
    return False


def _normalize_record(raw: dict) -> Tuple[Optional[dict], Optional[str]]:
    """
    Normalize provider records to a PositionReport shape.

    Accepted inputs:
    1) Existing AIS stream-like message with MessageType=PositionReport
    2) Flat provider record with mmsi/lat/lon/timestamp fields
    """
    if _is_position_report_aisstream_shape(raw):
        return raw, None

    mmsi = raw.get('mmsi') or raw.get('MMSI') or raw.get('ship_id') or raw.get('vessel_id')
    lat = raw.get('lat') or raw.get('latitude') or raw.get('Latitude')
    lon = raw.get('lon') or raw.get('longitude') or raw.get('Longitude')
    ts = raw.get('timestamp') or raw.get('time') or raw.get('ts')

    if mmsi is None or lat is None or lon is None:
        return None, 'Missing required fields for normalization (mmsi/lat/lon).'

    normalized = {
        'MessageType': 'PositionReport',
        'MetaData': {
            'MMSI': mmsi,
        },
        'Message': {
            'PositionReport': {
                'Latitude': lat,
                'Longitude': lon,
                'Timestamp': ts,
            }
        }
    }
    return normalized, None


def _record_fingerprint(msg: dict) -> str:
    md = msg.get('MetaData', {}) if isinstance(msg, dict) else {}
    pr = msg.get('Message', {}).get('PositionReport', {}) if isinstance(msg, dict) else {}
    return '|'.join([
        str(md.get('MMSI', '')),
        str(pr.get('Timestamp', '')),
        str(pr.get('Latitude', '')),
        str(pr.get('Longitude', '')),
    ])


def run_http_ingestion(
    url: str,
    poll_interval_sec: int,
    timeout_sec: int,
    retry_attempts: int,
    retry_backoff_sec: int,
    max_cycles: int,
):
    producer = None
    dlq = None
    recent_keys: List[str] = []
    recent_keys_set = set()
    max_recent_keys = 10000
    cycles = 0

    try:
        producer = AISProducerWrapper()
        dlq = DeadLetterHandler()
    except Exception as exc:
        logger.critical(f'Failed to initialize Kafka components: {exc}')
        return

    logger.info(f'Starting HTTP AIS ingestion from {url}')

    while not _SHUTDOWN:
        if max_cycles > 0 and cycles >= max_cycles:
            logger.info(f'Reached max_cycles={max_cycles}, stopping.')
            break

        cycles += 1
        published = 0
        invalid = 0

        try:
            payload = _fetch_json(
                url=url,
                timeout_sec=timeout_sec,
                retry_attempts=retry_attempts,
                retry_backoff_sec=retry_backoff_sec,
            )
            records = _extract_records(payload)

            for record in records:
                normalized, err = _normalize_record(record)
                if err:
                    invalid += 1
                    dlq.handle_invalid_message(json.dumps(record), err)
                    continue

                key = _record_fingerprint(normalized)
                if key in recent_keys_set:
                    continue

                producer.publish_record(normalized)
                published += 1

                recent_keys.append(key)
                recent_keys_set.add(key)
                if len(recent_keys) > max_recent_keys:
                    oldest = recent_keys.pop(0)
                    recent_keys_set.discard(oldest)

            logger.info(
                f'Cycle {cycles}: fetched={len(records)}, published={published}, invalid={invalid}'
            )

        except Exception as exc:
            logger.error(f'HTTP ingestion cycle failed: {exc}')

        if not _SHUTDOWN:
            time.sleep(poll_interval_sec)

    if producer:
        producer.close()
    if dlq:
        dlq.close()
    logger.info('HTTP AIS ingestion stopped cleanly.')


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='Poll AIS provider HTTP API and publish PositionReports to Kafka.')
    parser.add_argument('--url', default=os.getenv('AIS_HTTP_URL', ''), help='AIS provider HTTP endpoint URL')
    parser.add_argument('--poll-interval', type=int, default=int(os.getenv('AIS_HTTP_POLL_INTERVAL_SEC', '30')))
    parser.add_argument('--timeout', type=int, default=int(os.getenv('AIS_HTTP_TIMEOUT_SEC', '20')))
    parser.add_argument('--retry-attempts', type=int, default=int(os.getenv('AIS_HTTP_RETRY_ATTEMPTS', '3')))
    parser.add_argument('--retry-backoff', type=int, default=int(os.getenv('AIS_HTTP_RETRY_BACKOFF_SEC', '2')))
    parser.add_argument('--max-cycles', type=int, default=0, help='0 means run forever')
    return parser.parse_args()


if __name__ == '__main__':
    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    args = _parse_args()
    if not args.url:
        logger.error('Missing AIS HTTP URL. Set AIS_HTTP_URL or pass --url.')
        sys.exit(1)

    run_http_ingestion(
        url=args.url,
        poll_interval_sec=args.poll_interval,
        timeout_sec=args.timeout,
        retry_attempts=args.retry_attempts,
        retry_backoff_sec=args.retry_backoff,
        max_cycles=args.max_cycles,
    )
