"""
Suspicious Event DAG.

This event-driven DAG consumes suspicious trigger events from Kafka by default
and can still run with manual dag_run.conf payloads for local testing.
It orchestrates the validation workflow: creating a bounding box, checking and downloading 
Sentinel-1 imagery, running the oil spill detection model, and updating the incident state.
"""

import os
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta, timezone
from orchestration.utils.state_store import StateStore, STATE_PROCESSING, STATE_VERIFIED, STATE_FAILED
from orchestration.utils.geometry import create_buffer_bbox, wkt_from_bbox
from orchestration.operators.sentinel_search import SentinelSearchOperator
from orchestration.operators.sentinel_download import SentinelDownloadOperator
from orchestration.operators.sar_inference import SARInferenceOperator
from orchestration.sensors.kafka_trigger_sensor import KafkaTriggerSensor

# Default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

KAFKA_BOOTSTRAP = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:29092')
SAR_TRIGGER_TOPIC = os.getenv('SAR_TRIGGER_TOPIC', 'sar.trigger.events')
SAR_TRIGGER_GROUP_ID = os.getenv('AIRFLOW_SAR_TRIGGER_GROUP_ID', 'airflow-sar-trigger-v1')
SAR_TRIGGER_POLL_TIMEOUT_MS = int(os.getenv('AIRFLOW_SAR_TRIGGER_POLL_TIMEOUT_MS', '1000'))


def _get_event_payload(context):
    """Read payload from dag_run.conf (manual) or Kafka sensor XCom (default)."""
    dag_run_conf = context['dag_run'].conf or {}
    if isinstance(dag_run_conf, dict) and dag_run_conf.get('incident_id'):
        return dag_run_conf

    ti = context['ti']
    sensor_payload = ti.xcom_pull(task_ids='wait_for_sar_trigger', key='return_value')
    if isinstance(sensor_payload, dict):
        return sensor_payload

    raise ValueError('No valid trigger payload found in dag_run.conf or Kafka sensor XCom.')

def initialize_incident(**context):
    """Initializes incident state from Kafka-trigger payload or dag_run.conf."""
    conf = _get_event_payload(context)
    incident_id = conf.get('incident_id')
    
    if not incident_id:
        raise ValueError("No incident_id provided in DAG run configuration.")
        
    store = StateStore()
    store.update_incident_state(incident_id, STATE_PROCESSING, metadata=conf)
    return incident_id

def prepare_search_params(**context):
    """Calculates ROI and date range for satellite search."""
    conf = _get_event_payload(context)
    lat = conf.get('lat')
    lon = conf.get('lon')
    event_time = conf.get('timestamp') # ISO string expected

    if lat is None or lon is None:
        raise ValueError("Both lat and lon must be provided in DAG run configuration.")

    if not event_time:
        raise ValueError("No timestamp provided in DAG run configuration.")

    # Create 20km buffer
    bbox = create_buffer_bbox(lat, lon, radius_km=20.0)
    wkt = wkt_from_bbox(bbox)
    
    normalized_event_time = event_time.replace('Z', '+00:00')
    try:
        event_dt = datetime.fromisoformat(normalized_event_time)
    except ValueError as exc:
        raise ValueError(f"Invalid timestamp format: {event_time}. Expected ISO-8601 format.") from exc

    if event_dt.tzinfo is None:
        event_dt = event_dt.replace(tzinfo=timezone.utc)
    else:
        event_dt = event_dt.astimezone(timezone.utc)

    # Search for images in a 48-hour window centered on the AIS event.
    start_date = (event_dt - timedelta(hours=24)).strftime('%Y-%m-%dT%H:%M:%SZ')
    end_date = (event_dt + timedelta(hours=24)).strftime('%Y-%m-%dT%H:%M:%SZ')
    
    return {
        "roi_wkt": wkt,
        "start_date": start_date,
        "end_date": end_date
    }

def process_results(**context):
    """Updates state based on inference results."""
    ti = context['ti']
    results = ti.xcom_pull(task_ids='sar_inference')
    conf = _get_event_payload(context)
    incident_id = conf.get('incident_id')
    
    store = StateStore()
    if not isinstance(results, list):
        store.update_incident_state(incident_id, STATE_FAILED, metadata={"reason": "Invalid inference result format"})
        return

    has_spill = any(isinstance(r, dict) and r.get('prediction') == 'oil_spill' for r in results)
    if has_spill:
        store.update_incident_state(incident_id, STATE_VERIFIED, metadata={"inference": results})
    else:
        # If no oil spill detected or no images, technically not 'FAILED' but 'CLEARED' or 'NO_DATA'
        # For this logic we keep it simple.
        store.update_incident_state(incident_id, STATE_FAILED, metadata={"reason": "No spill detected"})


with DAG(
    'suspicious_event_validation',
    default_args=default_args,
    description='Validates suspicious AIS events using Sentinel-1 imagery',
    schedule=None,
    start_date=datetime(2026, 5, 1, tzinfo=timezone.utc),
    catchup=False,
    tags=['event_driven', 'ais', 'oil_spill'],
) as dag:

    wait_for_trigger = KafkaTriggerSensor(
        task_id='wait_for_sar_trigger',
        topic=SAR_TRIGGER_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id=SAR_TRIGGER_GROUP_ID,
        poll_timeout_ms=SAR_TRIGGER_POLL_TIMEOUT_MS,
        poke_interval=30,
        timeout=60 * 30,
        mode='reschedule',
    )

    init_task = PythonOperator(
        task_id='initialize_incident',
        python_callable=initialize_incident
    )

    prepare_params_task = PythonOperator(
        task_id='prepare_search_params',
        python_callable=prepare_search_params
    )

    # Params are passed via Jinja-templated XCom pulls from prepare_search_params return payload.
    
    search_sentinel = SentinelSearchOperator(
        task_id='search_sentinel',
        roi_wkt="{{ ti.xcom_pull(task_ids='prepare_search_params', key='return_value')['roi_wkt'] }}",
        search_start="{{ ti.xcom_pull(task_ids='prepare_search_params', key='return_value')['start_date'] }}",
        search_end="{{ ti.xcom_pull(task_ids='prepare_search_params', key='return_value')['end_date'] }}",
    )

    download_sentinel = SentinelDownloadOperator(
        task_id='download_sentinel',
        download_dir='/tmp/sentinel_data'
    )

    sar_inference = SARInferenceOperator(
        task_id='sar_inference',
        model_path='/models/oil_spill_v1.pt'
    )
    
    finalize_task = PythonOperator(
        task_id='finalize_incident',
        python_callable=process_results
    )

    wait_for_trigger >> init_task >> prepare_params_task >> search_sentinel >> download_sentinel >> sar_inference >> finalize_task
