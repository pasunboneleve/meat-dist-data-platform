import os
from datetime import UTC, date, datetime, timedelta
from typing import Dict

from airflow.providers.google.cloud.operators.dataproc import \
    DataprocCreateBatchOperator
from airflow.providers.google.cloud.sensors.gcs import \
    GCSObjectsWithPrefixExistenceSensor
from airflow.sdk import dag, get_current_context, task

from assets import bronze_carcasses_asset, silver_dv2_asset

default_args = {
    "owner": "data-eng",
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}

# Define buckets once
BRONZE_BUCKET = os.environ.get("BRONZE_BUCKET")
SILVER_BUCKET = os.environ.get("SILVER_BUCKET")

if not all([BRONZE_BUCKET, SILVER_BUCKET]):
    raise ValueError("Missing BRONZE_BUCKET or SILVER_BUCKET env vars")


@dag(
    dag_id="bronze_to_silver_dv2",
    default_args=default_args,
    description="Bronze Parquet → Silver DV2 Iceberg via Dataproc Serverless Spark",
    schedule=[bronze_carcasses_asset],
    start_date=datetime(2024, 1, 1, tzinfo=UTC),
    catchup=False,
    tags=["transform", "silver", "dataproc", "iceberg"],
    max_active_runs=1,
)
def bronze_to_silver_dv2():
    @task
    def get_config(**context) -> Dict[str, str]:
        # 1. Get triggering asset events
        triggering_events = context.get("triggering_asset_events", {})

        if bronze_carcasses_asset not in triggering_events:
            raise ValueError(
                "No triggering events found for bronze_carcasses_asset. "
                "This DAG should only run when bronze asset is updated."
            )

        events = triggering_events[bronze_carcasses_asset]
        if not events:
            raise ValueError("Empty event list for bronze_carcasses_asset")

        # Take the most recent event
        latest_event = events[-1]

        # 2. Extract metadata from extra
        extra = latest_event.extra or {}
        target_date_str = extra.get("target_date_str")

        if not target_date_str:
            raise ValueError(
                "Upstream bronze asset event is missing 'target_date_str' in extra. "
                "Check that daily_synthetic_ingestion is correctly emitting metadata."
            )

        try:
            target_date = date.fromisoformat(target_date_str)
        except ValueError as e:
            raise ValueError(
                f"Invalid date format in metadata 'target_date_str': {target_date_str}"
            ) from e

        # 3. Rebuild prefix from date (consistent with producer)
        prefix = (
            f"carcasses/year={target_date.year}/"
            f"month={target_date.month:02d}/"
            f"day={target_date.day:02d}/"
        )

        print(f"Using upstream metadata date: {target_date_str}")
        print(f"Rebuilt prefix: {prefix}")

        return {
            "target_date_str": target_date_str,
            "target_prefix": prefix,
        }

    @task
    def submit_spark_transform(config: Dict[str, str]):
        return DataprocCreateBatchOperator(
            task_id="transform_dv2_iceberg",
            project_id=os.environ["GCP_PROJECT_ID"],
            region=os.environ["DATAPROC_REGION"],
            batch_id=(
                "bronze-to-silver-dv2-{{ ds_nodash }}-{{ ts_nodash | replace('T', '') "
                "| replace('+', '-') | lower }}-{{ ti.try_number }}"
            ),
            batch={
                "pyspark_batch": {
                    "main_python_file_uri": f"gs://{os.environ['DEPS_BUCKET']}/spark_jobs/transform_bronze_to_silver.py",
                    "jar_file_uris": [
                        f"gs://{os.environ['DEPS_BUCKET']}/iceberg-spark-runtime-3.5_2.13-1.10.1.jar",
                    ],
                },
                "runtime_config": {
                    "version": "2.2",
                    "properties": {
                        "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
                        "spark.sql.catalog.spark_catalog": "org.apache.iceberg.spark.SparkCatalog",
                        "spark.sql.catalog.spark_catalog.type": "hadoop",
                        "spark.sql.catalog.spark_catalog.warehouse": f"gs://{SILVER_BUCKET}/iceberg_warehouse/",
                        "spark.executor.instances": "2",
                        "spark.executor.cores": "4",
                        "spark.executor.memory": "4g",
                        "spark.driver.cores": "4",
                        "spark.driver.memory": "4g",
                        "spark.dynamicAllocation.enabled": "false",
                        "spark.sql.execution_date": "{{ ds }}",
                        "spark.sql.bronze_bucket": BRONZE_BUCKET,
                        "spark.sql.silver_bucket": SILVER_BUCKET,
                        "spark.sql.target_date_str": config["target_date_str"],
                    },
                },
                "environment_config": {
                    "execution_config": {
                        "service_account": os.environ["DATAPROC_BATCH_SERVICE_ACCOUNT"],
                    }
                },
                "labels": {
                    "dag_id": "bronze-to-silver-dv2",
                    "date": "{{ ds_nodash }}",
                    "run_id_hash": "{{ ts_nodash | replace('T', '') | replace('+', '-') | lower }}",
                    "try_number": "{{ ti.try_number }}",
                },
            },
            gcp_conn_id="google_cloud_default",
        )

    @task.sensor
    def verify_silver(config: Dict[str, str]):
        sensor = GCSObjectsWithPrefixExistenceSensor(
            task_id="verify_silver_tables",
            bucket=SILVER_BUCKET,  # type: ignore[arg-type]
            prefix="iceberg_warehouse/default/hub_carcass/metadata/",
            google_cloud_conn_id="google_cloud_default",
            timeout=300,
            poke_interval=30,
            mode="reschedule",
        )

        context = get_current_context()
        sensor.execute(context=context)

        return config

    # Chain
    config = get_config()
    spark_job = submit_spark_transform(config)  # type: ignore[arg-type]
    waited = verify_silver(spark_job)  # type: ignore[arg-type]

    @task(outlets=[silver_dv2_asset])
    def mark_silver_produced(config: Dict[str, str], **context):
        print(
            f"Silver DV2 Iceberg asset produced for date: {config['target_date_str']}"
        )

    mark_silver_produced(waited)  # type: ignore[arg-type]


bronze_to_silver_dv2()
