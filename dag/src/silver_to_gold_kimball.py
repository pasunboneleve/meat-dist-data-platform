import os
from datetime import UTC, date, datetime, timedelta
from typing import Dict

from airflow.providers.google.cloud.operators.dataproc import \
    DataprocCreateBatchOperator
from airflow.sdk import Asset, dag, task

default_args = {
    "owner": "data-eng",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Define assets
SILVER_BUCKET = os.environ.get("SILVER_BUCKET")
GOLD_BUCKET = os.environ.get("GOLD_BUCKET")

silver_dv2_asset = Asset(
    uri=f"gcs://{SILVER_BUCKET}/iceberg_warehouse",
    name="silver_dv2_iceberg",
    extra={"layer": "silver", "format": "iceberg"},
)

gold_kimball_asset = Asset(
    uri=f"gcs://{GOLD_BUCKET}/fact_carcass_transactions",
    name="gold_kimball_fact_carcass_transactions",
    extra={"layer": "gold", "model": "kimball"},
)


@dag(
    dag_id="silver_to_gold_kimball",
    default_args=default_args,
    description="Silver Iceberg → Gold Kimball transform via Dataproc Serverless Spark",
    schedule=[silver_dv2_asset],  # triggers when silver asset is updated
    start_date=datetime(2024, 1, 1, tzinfo=UTC),
    catchup=False,
    tags=["transform", "gold", "dataproc", "iceberg", "kimball"],
    max_active_runs=1,
)
def silver_to_gold_kimball():
    @task
    def get_config(**context) -> Dict[str, str]:
        logical_date: date = context["logical_date"].date() - timedelta(days=1)
        target_date_str = logical_date.isoformat()
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
                        "spark.sql.catalog.spark_catalog.warehouse": f"gs://{SILVER_BUCKET}/iceberg_warehouse/",
                        "spark.sql.execution_date": "{{ ds }}",
                        "spark.sql.gold_bucket": os.environ["GOLD_BUCKET"],
                        "spark.sql.silver_bucket": os.environ["SILVER_BUCKET"],
                        "spark.sql.target_date_str": config[
                            "target_date_str"
                        ],  # ← resolved from XCom
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
        # Optional: lightweight post-checks, Slack/email notification, etc.

    mark_gold_produced(spark_job)  # type: ignore[arg-type]


silver_to_gold_kimball()
