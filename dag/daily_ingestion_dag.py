import os
from datetime import datetime, timedelta

import requests
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.sensors.gcs import \
    GCSObjectsWithPrefixExistenceSensor
from requests.exceptions import RequestException

default_args = {
    "owner": "data-eng",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    dag_id="daily_synthetic_ingestion",
    default_args=default_args,
    description="Daily trigger for synthetic meat ingestion to Bronze GCS",
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["ingestion", "bronze"],
)


def trigger_synthetic_meat_ingestion(**context):
    url = os.environ["SYNTHETIC_MEAT_URL"].strip().rstrip("/")

    try:
        response = requests.post(url, json={}, timeout=300)  # add payload if needed
        response.raise_for_status()
        print(f"Ingestion triggered successfully: {response.status_code}")
        if response.content:
            print(response.text)
    except RequestException as e:
        raise Exception(f"Failed to trigger synthetic meat ingestor at {url}: {e}")


trigger_ingestion = PythonOperator(
    task_id="trigger_synthetic_ingestor",
    python_callable=trigger_synthetic_meat_ingestion,
    dag=dag,
)

wait_bronze = GCSObjectsWithPrefixExistenceSensor(
    task_id="wait_for_bronze_data",
    bucket="{{ var.value.bronze_bucket }}",  # note: this uses Airflow Variable (still supported)
    prefix="carcasses/",
    google_cloud_conn_id="google_cloud_default",
    timeout=7200,
    poke_interval=600,
    dag=dag,
)

end = EmptyOperator(task_id="end", dag=dag)

trigger_ingestion >> wait_bronze >> end
