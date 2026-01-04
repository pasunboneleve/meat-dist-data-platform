import os
from datetime import datetime, timedelta

import requests
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.sensors.gcs import \
    GCSObjectsWithPrefixExistenceSensor
from google.auth.transport.requests import Request
from google.oauth2 import id_token

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
        # Fetch ID token for the specific Cloud Run audience
        id_token_token = id_token.fetch_id_token(Request(), audience=url)

        headers = {
            "Authorization": f"Bearer {id_token_token}",
            "Content-Type": "application/json",
        }

        response = requests.post(url, json={}, headers=headers, timeout=300)
        response.raise_for_status()

        print(f"Ingestion triggered successfully: {response.status_code}")
        if response.content:
            print(response.text)

    except requests.exceptions.HTTPError as e:
        raise Exception(
            f"Ingestion trigger failed with status {response.status_code}: {response.text}"
        ) from e
    except Exception as e:
        raise Exception(f"Failed to trigger ingestor: {e}") from e


# Task 1: Read the bucket from Composer environment variable and push to XCom
def get_bronze_bucket(**context):
    bucket = os.environ.get("BRONZE_BUCKET")
    if not bucket:
        raise ValueError("Environment variable BRONZE_BUCKET is not set in Composer")
    print(f"Retrieved bronze bucket from env: {bucket}")

    # Push to XCom so downstream tasks can access it
    context["ti"].xcom_push(key="bronze_bucket", value=bucket)
    return bucket  # also returned for convenience (same as push)


get_bucket_task = PythonOperator(
    task_id="get_bronze_bucket",
    python_callable=get_bronze_bucket,
    dag=dag,
)

# Task 2: Trigger ingestion
trigger_ingestion = PythonOperator(
    task_id="trigger_synthetic_ingestor",
    python_callable=trigger_synthetic_meat_ingestion,
    dag=dag,
)

# Task 3: GCS sensor using the bucket from XCom
wait_bronze = GCSObjectsWithPrefixExistenceSensor(
    task_id="wait_for_bronze_data",
    bucket="{{ ti.xcom_pull(task_ids='get_bronze_bucket') }}",
    prefix="carcasses/",
    google_cloud_conn_id="google_cloud_default",
    timeout=7200,
    poke_interval=600,
    dag=dag,
)

end = EmptyOperator(task_id="end", dag=dag)

trigger_ingestion >> wait_bronze >> end
