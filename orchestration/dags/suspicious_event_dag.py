"""
Suspicious Event DAG.

This event-driven DAG is triggered externally when a suspicious AIS event is detected.
It orchestrates the validation workflow: creating a bounding box, checking and downloading 
Sentinel-1 imagery, running the oil spill detection model, and updating the incident state.
"""

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
from orchestration.utils.state_store import StateStore, STATE_PROCESSING, STATE_VERIFIED, STATE_FAILED
from orchestration.utils.geometry import create_buffer_bbox, wkt_from_bbox
from orchestration.operators.sentinel_search import SentinelSearchOperator
from orchestration.operators.sentinel_download import SentinelDownloadOperator
from orchestration.operators.sar_inference import SARInferenceOperator

# Default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def initialize_incident(**context):
    """Initializes the incident state directly from DAG run configuration."""
    conf = context['dag_run'].conf
    incident_id = conf.get('incident_id')
    
    if not incident_id:
        raise ValueError("No incident_id provided in DAG run configuration.")
        
    store = StateStore()
    store.update_incident_state(incident_id, STATE_PROCESSING, metadata=conf)
    return incident_id

def prepare_search_params(**context):
    """Calculates ROI and date range for satellite search."""
    conf = context['dag_run'].conf
    lat = conf.get('lat')
    lon = conf.get('lon')
    event_time = conf.get('timestamp') # ISO string expected

    # Create 20km buffer
    bbox = create_buffer_bbox(lat, lon, radius_km=20.0)
    wkt = wkt_from_bbox(bbox)
    
    # Simple date logic: search for images +/- 24 hours around event
    # In a real app we'd parse event_time properly
    start_date = "2023-10-24T00:00:00Z" 
    end_date = "2023-10-26T00:00:00Z"
    
    return {
        "roi_wkt": wkt,
        "start_date": start_date,
        "end_date": end_date
    }

def process_results(**context):
    """Updates state based on inference results."""
    ti = context['ti']
    results = ti.xcom_pull(task_ids='sar_inference')
    conf = context['dag_run'].conf
    incident_id = conf.get('incident_id')
    
    store = StateStore()
    if results and any(r['prediction'] == 'oil_spill' for r in results):
        store.update_incident_state(incident_id, STATE_VERIFIED, metadata={"inference": results})
    else:
        # If no oil spill detected or no images, technically not 'FAILED' but 'CLEARED' or 'NO_DATA'
        # For this logic we keep it simple.
        store.update_incident_state(incident_id, STATE_FAILED, metadata={"reason": "No spill detected"})


with DAG(
    'suspicious_event_validation',
    default_args=default_args,
    description='Validates suspicious AIS events using Sentinel-1 imagery',
    schedule_interval=None, # Triggered externally
    start_date=days_ago(1),
    tags=['event_driven', 'ais', 'oil_spill'],
) as dag:

    init_task = PythonOperator(
        task_id='initialize_incident',
        python_callable=initialize_incident,
        provide_context=True
    )

    prepare_params_task = PythonOperator(
        task_id='prepare_search_params',
        python_callable=prepare_search_params,
        provide_context=True
    )

    # Note: Params passed via XCom/Op args would need templating or dynamic mapping in a real complex setup.
    # For simplicity, we assume the operator grabs XCom from prepare_params_task or we pass specific args.
    # Airflow templates {{ task_instance.xcom_pull(...) }} are standard.
    
    search_sentinel = SentinelSearchOperator(
        task_id='search_sentinel',
        roi_wkt="{{ task_instance.xcom_pull(task_ids='prepare_search_params')['roi_wkt'] }}",
        start_date="{{ task_instance.xcom_pull(task_ids='prepare_search_params')['start_date'] }}",
        end_date="{{ task_instance.xcom_pull(task_ids='prepare_search_params')['end_date'] }}",
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
        python_callable=process_results,
        provide_context=True
    )

    init_task >> prepare_params_task >> search_sentinel >> download_sentinel >> sar_inference >> finalize_task
