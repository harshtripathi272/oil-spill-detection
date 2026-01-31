"""
Sentinel Polling DAG.

This DAG runs on a schedule to check for Sentinel-1 imagery availability 
for specific areas of interest (AOIs). It is useful for persistent monitoring 
independent of specific AIS events.
"""

from airflow import DAG
from airflow.sensors.base import BaseSensorOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
from orchestration.sensors.sentinel_availability_sensor import SentinelAvailabilitySensor

# Default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=10),
}

def notify_availability(**context):
    """Simple notification function."""
    print("Sentinel-1 data is available for the monitored region.")

# Define a static polling region (e.g., a known shipping lane or sensitive zone)
MONITORED_ROI_BBOX = [10.0, 45.0, 11.0, 46.0] # Example coordinates

with DAG(
    'sentinel_polling',
    default_args=default_args,
    description='Polls for Sentinel-1 availability in key regions',
    schedule_interval='@daily',
    start_date=days_ago(1),
    catchup=False,
    tags=['monitoring', 'sentinel'],
) as dag:

    start = DummyOperator(task_id='start')

    wait_for_data = SentinelAvailabilitySensor(
        task_id='wait_for_sentinel_data',
        roi_bbox=MONITORED_ROI_BBOX,
        date_range=("{{ ds }}", "{{ next_ds }}"), # Check for data generated today
        poke_interval=60 * 60, # Check every hour
        timeout=60 * 60 * 24, # Timeout after 24 hours
        mode='reschedule' # Release slot while waiting
    )

    notify = PythonOperator(
        task_id='notify_data_available',
        python_callable=notify_availability,
        provide_context=True
    )
    
    end = DummyOperator(task_id='end')

    start >> wait_for_data >> notify >> end
