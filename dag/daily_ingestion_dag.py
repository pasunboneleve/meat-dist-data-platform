import os
from datetime import UTC, datetime, timedelta
from typing import Any, Dict

import requests
from airflow import DAG
from airflow.providers.google.cloud.sensors.gcs import \
    GCSObjectsWithPrefixExistenceSensor
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.trigger_dagrun import \
    TriggerDagRunOperator
from airflow.sdk import task
from google.auth.transport.requests import Request
from google.cloud.storage._helpers import _validate_name
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
    start_date=datetime(2024, 1, 1, tzinfo=UTC),
    catchup=False,
    tags=["ingestion", "bronze"],
)


def yesterday(context: Dict[str, Any]) -> Dict[str, str]:
    """
    Create a JSON payload to fetch yesterday's stats.
    """
    target_date = context["logical_date"].date() - timedelta(days=1)
    from_date_str = target_date.strftime("%Y-%m-%d")
    to_date_str = target_date.strftime("%Y-%m-%d")
    return {"from_date": from_date_str, "to_date": to_date_str}


@task(dag=dag)
def trigger_synthetic_meat_ingestion(**context):
    url = os.environ["SYNTHETIC_MEAT_URL"].strip().rstrip("/")
    payload = yesterday(context)
    response = None

    try:
        # Fetch ID token for the specific Cloud Run audience
        id_token_token = id_token.fetch_id_token(Request(), audience=url)

        headers = {
            "Authorization": f"Bearer {id_token_token}",
            "Content-Type": "application/json",
        }

        response = requests.post(url, json=payload, headers=headers, timeout=300)
        response.raise_for_status()

        print(f"Ingestion triggered successfully: {response.status_code}")
        if response.content:
            print(response.text)

    except requests.exceptions.HTTPError as e:
        if response:
            raise Exception(
                f"Ingestion trigger failed \
with status {response.status_code}: {response.text}"
            ) from e
        else:
            raise Exception(f"Ingestion failed with {e}")
    except Exception as e:
        raise Exception(f"Failed to trigger ingestor: {e}") from e


@task
def set_bucket():
    bucket = os.environ.get("BRONZE_BUCKET")
    if not bucket:
        raise ValueError(
            "Environment variable BRONZE_BUCKET \
is not set in Composer"
        )
    print(f"Retrieved bronze bucket from env: {bucket}")
    _validate_name(bucket)  # raises ValueError early if invalid
    return bucket


get_bucket_task = set_bucket()

# Task 2: Trigger ingestion
trigger_ingestion = trigger_synthetic_meat_ingestion()

# Task 3: GCS sensor using the bucket from XCom
wait_bronze = GCSObjectsWithPrefixExistenceSensor(
    task_id="wait_for_bronze_data",
    bucket="{{ ti.xcom_pull(task_ids='set_bucket') }}",
    prefix="carcasses/",
    google_cloud_conn_id="google_cloud_default",
    timeout=7200,
    poke_interval=600,
    dag=dag,
)

# Task 4: Trigger Silver transform DAG
trigger_transform = TriggerDagRunOperator(
    task_id="trigger_bronze_to_silver",
    trigger_dag_id="bronze_to_silver_dv2",
    wait_for_completion=True,
    dag=dag,
)

end = EmptyOperator(task_id="end", dag=dag)

get_bucket_task >> trigger_ingestion >> wait_bronze >> trigger_transform >> end
