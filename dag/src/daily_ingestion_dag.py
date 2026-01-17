import os
from datetime import UTC, date, datetime, timedelta

import requests
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.sdk import Context, PokeReturnValue, dag, task
from google.auth.transport.requests import Request
from google.oauth2 import id_token

from asset_utils import emit_asset_with_metadata
from assets import bronze_carcasses_asset

# Env vars – set these in Airflow UI (Admin > Variables) or container env
BRONZE_BUCKET = os.environ.get("BRONZE_BUCKET")
SYNTHETIC_MEAT_URL = os.environ.get("SYNTHETIC_MEAT_URL")

if not all([BRONZE_BUCKET, SYNTHETIC_MEAT_URL]):
    raise ValueError(
        "Missing required env vars: BRONZE_BUCKET and/or SYNTHETIC_MEAT_URL"
    )

default_args = {
    "owner": "data-eng",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    dag_id="daily_synthetic_ingestion",
    default_args=default_args,
    description="Daily synthetic meat ingestion to Bronze GCS + wait for files",
    schedule="@daily",
    start_date=datetime(2024, 1, 1, tzinfo=UTC),
    catchup=False,
    tags=["ingestion", "bronze", "gcs"],
    max_active_runs=1,  # prevent overlapping runs if needed
)
def daily_synthetic_ingestion():
    @task
    def get_config(**context: Context) -> dict:
        logical_date: date = context["logical_date"].date()  # type: ignore[assignment]
        target_date_str = logical_date.isoformat()
        prefix = (
            f"carcasses/year={logical_date.year}/"
            f"month={logical_date.month:02d}/"
            f"day={logical_date.day:02d}/"
        )
        return {
            "target_date_str": target_date_str,
            "to_date_str": target_date_str,
            "target_prefix": prefix,
        }

    @task
    def trigger_synthetic_meat_ingestion(config: dict) -> dict:
        url = SYNTHETIC_MEAT_URL.strip().rstrip("/")  # type: ignore[arg-type]

        payload = {
            "from_date": config["target_date_str"],
            "to_date": config["to_date_str"],
        }
        print(f"Triggering ingestion for {payload}")

        try:
            id_token_token = id_token.fetch_id_token(Request(), audience=url)
            headers = {
                "Authorization": f"Bearer {id_token_token}",
                "Content-Type": "application/json",
            }

            response = requests.post(url, json=payload, headers=headers, timeout=300)
            response.raise_for_status()

            print(f"Triggered successfully: {response.status_code}")
            if response.content:
                print(response.text)

            return config  # pass config downstream

        except Exception as e:
            raise RuntimeError(f"Ingestion trigger failed: {e}") from e

    @task.sensor(
        poke_interval=600,
        timeout=7200,
        mode="reschedule",
    )
    def wait_for_bronze_data(config: dict) -> PokeReturnValue:
        hook = GCSHook(gcp_conn_id="google_cloud_default")

        # Poke logic: check if prefix has objects
        objects = hook.list(
            bucket_name=BRONZE_BUCKET,  # type: ignore[arg-type]
            prefix=config["target_prefix"],
            max_results=1,
        )
        exists = bool(objects)

        if exists:
            # Success → push config as XCom value
            return PokeReturnValue(is_done=True, xcom_value=config)

        return PokeReturnValue(is_done=False)

    @task(outlets=[bronze_carcasses_asset])
    def mark_asset_produced(config: dict[str, str], **context):
        print(f"Bronze asset produced for date: {config['target_date_str']}")

        emit_asset_with_metadata(
            context=context,
            asset=bronze_carcasses_asset,
            extra={
                "target_date_str": config["target_date_str"],
                "target_prefix": config.get("target_prefix"),
            },
            log_prefix=f"Emitted {bronze_carcasses_asset.name}",
        )

    # Chain tasks – TaskFlow passes config via XCom automatically

    config = get_config()
    triggered = trigger_synthetic_meat_ingestion(config)  # type: ignore[arg-type]
    waited = wait_for_bronze_data(triggered)  # type: ignore[arg-type]
    mark_asset_produced(waited)  # type: ignore[arg-type]


daily_synthetic_ingestion()
