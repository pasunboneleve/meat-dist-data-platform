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
    """
    Triggers the synthetic meat ingestor Cloud Run service.
    Fails the task on any HTTP status other than 2xx (e.g., 403, 404, 500).
    """
    url = os.environ["SYNTHETIC_MEAT_URL"].strip().rstrip("/")

    try:
        response = requests.post(
            url,
            json={},  # add payload if your endpoint expects one
            timeout=300,
        )

        # This line is critical: raises HTTPError for 4xx/5xx responses
        response.raise_for_status()

        print(f"Ingestion triggered successfully: {response.status_code}")
        if response.content:
            print("Response body:", response.text)

    except requests.exceptions.HTTPError as e:
        # Specific handling for HTTP errors (403, 404, etc.)
        raise Exception(
            f"Ingestion trigger failed with status {response.status_code}: {response.text}"
        ) from e

    except requests.exceptions.RequestException as e:
        # Handles timeout, connection error, etc.
        raise Exception(f"Failed to reach ingestor at {url}: {e}") from e


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
