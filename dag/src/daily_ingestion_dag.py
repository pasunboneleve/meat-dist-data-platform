import os
from datetime import UTC, date, datetime, timedelta
from typing import Any, Dict

import requests
from airflow import DAG
from airflow.models.dataset import Dataset
from airflow.providers.google.cloud.sensors.gcs import \
    GCSObjectsWithPrefixExistenceSensor
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import task
from google.auth.transport.requests import Request
from google.oauth2 import id_token

bronze_carcasses_dataset = Dataset(f"gcs://{os.environ.get('BRONZE_BUCKET')}/carcasses")

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


@task(dag=dag)
def get_config(**context: Dict[str, Any]) -> Dict[str, str]:
    """Load config from Airflow Variables."""
    logical_date: date = context["logical_date"].date() - timedelta(days=1)  # type: ignore
    target_date_str = logical_date.isoformat()
    prefix = f"carcasses/year={logical_date.year}/month={logical_date.month}/day={logical_date.day}/"
    return {
        "from_date_str": target_date_str,
        "to_date_str": target_date_str,
        "target_prefix": prefix,
    }


config = get_config()


@task(dag=dag)
def trigger_synthetic_meat_ingestion(**context: Dict[str, Any]) -> None:
    config = context["ti"].xcom_pull(task_ids="get_config")  # type: ignore
    url = os.environ["SYNTHETIC_MEAT_URL"].strip().rstrip("/")
    payload = {
        "from_date": config["from_date_str"],
        "to_date": config["to_date_str"],
    }
    print(f"Triggering ingestion with payload: {payload}")
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
                f"Ingestion trigger failed "
                f"with status {response.status_code}: {response.text}"
            ) from e
        else:
            raise Exception(f"Ingestion failed with {e}")
    except Exception as e:
        raise Exception(f"Failed to trigger ingestor: {e}") from e


trigger_ingestion = trigger_synthetic_meat_ingestion()

# Task 3: GCS sensor using the bucket from XCom
wait_bronze = GCSObjectsWithPrefixExistenceSensor(
    task_id="wait_for_bronze_data",
    bucket=os.environ["BRONZE_BUCKET"],
    prefix="{{ ti.xcom_pull(task_ids='get_config')['target_prefix'] }}",
    google_cloud_conn_id="google_cloud_default",
    timeout=7200,
    poke_interval=600,
    dag=dag,
    outlets=[bronze_carcasses_dataset],
)

end = EmptyOperator(task_id="end", dag=dag)

config >> trigger_ingestion >> wait_bronze >> end
