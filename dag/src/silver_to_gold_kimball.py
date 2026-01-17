import os
from datetime import UTC, datetime, timedelta
from typing import Dict

from airflow.providers.google.cloud.operators.dataproc import \
    DataprocCreateBatchOperator
from airflow.sdk import dag, get_current_context, task

from asset_utils import emit_asset_with_metadata
from assets import gold_kimball_asset, silver_dv2_asset
from config_utils import generate_dataproc_batch_id, get_target_config

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
        return get_target_config(
            context=context,
            upstream_asset=silver_dv2_asset,
            date_offset_days=0,  # example: same day for silver→gold
            metadata_key="target_date_str",
        )

    @task
    def submit_spark_transform_gold(config: Dict[str, str]):
        """Submits the Silver to Gold Spark batch job."""
        batch_id = generate_dataproc_batch_id(
            prefix="silver-to-gold",
        )
        operator = DataprocCreateBatchOperator(
            task_id="transform_kimball_gold",
            project_id=os.environ["GCP_PROJECT_ID"],
            region=os.environ["DATAPROC_REGION"],
            batch_id=batch_id,
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
                "labels": {
                    "layer": "silver-to-gold",
                },
            },
            gcp_conn_id="google_cloud_default",
        )
        context = get_current_context()
        operator.execute(context=context)
        return config

    # Chain tasks
    config = get_config()
    spark_job = submit_spark_transform_gold(config)  # type: ignore[arg-type]

    @task
    def mark_gold_produced(config: Dict[str, str], **context):
        """Marks the gold asset as produced after Spark job success."""
        print(f"Gold Kimball fact table produced for date: {config['target_date_str']}")
        emit_asset_with_metadata(
            context=context,
            asset=gold_kimball_asset,
            extra={"target_date_str": config["target_date_str"]},
            log_prefix=f"Emitted {gold_kimball_asset.name}",
        )
        # Optional: lightweight post-checks, notifications, etc.

    mark_gold_produced(spark_job)  # type: ignore[arg-type]


silver_to_gold_kimball()
