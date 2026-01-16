import os
from datetime import UTC, date, datetime, timedelta
from typing import Dict

from airflow.providers.google.cloud.operators.dataproc import \
    DataprocCreateBatchOperator
from airflow.sdk import dag, task

from assets import gold_kimball_asset, silver_dv2_asset

default_args = {
    "owner": "data-eng",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    dag_id="silver_to_gold_kimball",
    default_args=default_args,
    description="Silver Iceberg → Gold Kimball (asset-triggered or manual)",
    schedule=[silver_dv2_asset],  # ← only asset updates trigger automatically
    start_date=datetime(2024, 1, 1, tzinfo=UTC),
    catchup=False,
    tags=["transform", "gold", "dataproc", "iceberg", "kimball"],
    max_active_runs=1,
)
def silver_to_gold_kimball():
    @task
    def get_config(**context) -> Dict[str, str]:
        logical_date = context.get("logical_date")  # None in pure asset-triggered runs

        if logical_date is not None:
            # Manual trigger (you picked a logical date in UI) → use it
            target_date = logical_date.date() - timedelta(days=1)
        else:
            # Asset-triggered run → fallback to approximate date from run creation time
            dag_run = context["dag_run"]
            fallback_ts = dag_run.queued_at or dag_run.created_at
            target_date = (fallback_ts - timedelta(days=1)).date()

            # Optional improvement: Try to get exact date from upstream asset event metadata
            # (add this in your silver producer DAG for accuracy)
            triggering_events = context.get("triggering_asset_events", {})
            if silver_dv2_asset in triggering_events:
                events = triggering_events[silver_dv2_asset]
                if events:
                    latest_event = events[-1]
                    md_date = latest_event.metadata.get("target_date_str")
                    if md_date:
                        target_date = date.fromisoformat(md_date)

        target_date_str = target_date.isoformat()

        return {
            "target_date_str": target_date_str,
        }

    @task
    def submit_spark_transform_gold(config: Dict[str, str]):
        """Submits the Silver to Gold Spark batch job."""
        return DataprocCreateBatchOperator(
            task_id="transform_kimball_gold",
            project_id=os.environ["GCP_PROJECT_ID"],
            region=os.environ["DATAPROC_REGION"],
            batch_id=(
                "silver-to-gold-{{ ds_nodash }}-{{ ts_nodash | replace('T', '') "
                "| replace('+', '-') | lower }}-{{ ti.try_number }}"
            ),
            batch={
                "pyspark_batch": {
                    "main_python_file_uri": f"gs://{os.environ['DEPS_BUCKET']}/spark_jobs/transform_silver_to_gold.py",
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
                        "spark.sql.catalog.spark_catalog.warehouse": f"gs://{os.environ['SILVER_BUCKET']}/iceberg_warehouse/",
                        "spark.sql.execution_date": "{{ ds }}",
                        "spark.sql.gold_bucket": os.environ["GOLD_BUCKET"],
                        "spark.sql.silver_bucket": os.environ["SILVER_BUCKET"],
                        "spark.sql.target_date_str": config["target_date_str"],
                    },
                },
                "environment_config": {
                    "execution_config": {
                        "service_account": os.environ["DATAPROC_BATCH_SERVICE_ACCOUNT"],
                    }
                },
            },
            gcp_conn_id="google_cloud_default",
        )

    # Chain tasks
    config = get_config()
    spark_job = submit_spark_transform_gold(config)  # type: ignore[arg-type]

    @task(outlets=[gold_kimball_asset])
    def mark_gold_produced(config: Dict[str, str]):
        """Marks the gold asset as produced after Spark job success."""
        print(f"Gold Kimball fact table produced for date: {config['target_date_str']}")
        # Optional: lightweight post-checks, notifications, etc.

    mark_gold_produced(spark_job)  # type: ignore[arg-type]


silver_to_gold_kimball()
