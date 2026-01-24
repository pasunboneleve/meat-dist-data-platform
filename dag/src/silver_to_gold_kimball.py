import os
from datetime import UTC, datetime, timedelta
from typing import Dict

from airflow.models.taskinstance import TaskInstance
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.operators.dataproc import \
    DataprocCreateBatchOperator
from airflow.sdk import PokeReturnValue, dag, task
from airflow.sdk.definitions.asset.metadata import Metadata

from utils.assets import gold_kimball_asset, silver_dv2_asset
from utils.config import generate_dataproc_batch_id, get_config_from_trigger

default_args = {
    "owner": "data-eng",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

# Define buckets once
SILVER_BUCKET = os.environ.get("SILVER_BUCKET")
GOLD_BUCKET = os.environ.get("GOLD_BUCKET")

if not all([SILVER_BUCKET, GOLD_BUCKET]):
    raise ValueError("Missing SILVER_BUCKET or GOLD_BUCKET env vars")


@dag(
    dag_id="silver_to_gold_kimball",
    default_args=default_args,
    description="Silver Iceberg → Gold Kimball (asset-triggered or manual)",
    schedule=[silver_dv2_asset],
    start_date=datetime(2024, 1, 1, tzinfo=UTC),
    catchup=False,
    tags=["transform", "gold", "dataproc", "iceberg", "kimball"],
    max_active_runs=1,
    user_defined_macros={
        "utils": {"config": {"generate_dataproc_batch_id": generate_dataproc_batch_id}}
    },
)
def silver_to_gold_kimball():
    @task
    def get_config(**context) -> Dict[str, str]:
        return get_config_from_trigger(
            context=context,
            upstream_asset=silver_dv2_asset,
        )

    config_task = get_config()

    submit_spark_transform = DataprocCreateBatchOperator(
        task_id="submit_spark_transform",
        project_id=os.environ["GCP_PROJECT_ID"],
        region=os.environ["DATAPROC_REGION"],
        batch_id="{{ utils.config.generate_dataproc_batch_id(prefix='silver-to-gold-kimball') }}",
        batch={
            "pyspark_batch": {
                "main_python_file_uri": f"gs://{os.environ['DEPS_BUCKET']}/spark_jobs/transform_silver_to_gold.py",
            },
            "runtime_config": {
                "version": "2.2",  # Runs Spark 3.5
                "properties": {
                    # 1. Standard Iceberg Dependencies (Maven)
                    "spark.jars.packages": "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.0,org.apache.iceberg:iceberg-gcp-bundle:1.6.0",
                    # 2. VALID GCS ARTIFACT: Point to the 'iceberg-bigquery-catalog' jar
                    "spark.jars": "gs://spark-lib/bigquery/iceberg-bigquery-catalog-1.6.1-1.0.2.jar",
                    "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
                    "spark.sql.iceberg.merge-schema": "true",
                    # 3. CATALOG CONFIG
                    "spark.sql.catalog.biglake": "org.apache.iceberg.spark.SparkCatalog",
                    "spark.sql.catalog.biglake.catalog-impl": "org.apache.iceberg.gcp.bigquery.BigQueryMetastoreCatalog",
                    "spark.sql.catalog.biglake.gcp_project": os.environ[
                        "GCP_PROJECT_ID"
                    ],
                    "spark.sql.catalog.biglake.gcp_location": os.environ[
                        "DATAPROC_REGION"
                    ],
                    "spark.sql.catalog.biglake.blms_catalog": os.environ[
                        "CATALOG_NAME"
                    ],
                    "spark.sql.catalog.biglake.warehouse": f"gs://{GOLD_BUCKET}/iceberg_warehouse",
                    "spark.sql.catalog.biglake.io-impl": "org.apache.iceberg.gcp.gcs.GCSFileIO",
                    # Performance & Resources
                    "spark.executor.instances": "2",
                    "spark.executor.cores": "4",
                    "spark.executor.memory": "4g",
                    "spark.driver.cores": "4",
                    "spark.driver.memory": "4g",
                    "spark.dynamicAllocation.enabled": "false",
                    # Job Arguments
                    "spark.sql.execution_date": "{{ ti.xcom_pull(task_ids='get_config')['target_date_str'] }}",
                    "spark.sql.silver_bucket": SILVER_BUCKET,
                    "spark.sql.gold_bucket": GOLD_BUCKET,
                    "spark.sql.target_date_str": "{{ ti.xcom_pull(task_ids='get_config')['target_date_str'] }}",
                    "spark.sql.gcp_project": os.environ["GCP_PROJECT_ID"],
                    "spark.sql.dataproc_region": os.environ["DATAPROC_REGION"],
                    "spark.sql.catalog_name": os.environ["CATALOG_NAME"],
                    "spark.sql.silver_db_name": os.environ["SILVER_DB_NAME"],
                    "spark.sql.gold_db_name": os.environ.get(
                        "GOLD_DB_NAME", "gold"
                    ),
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

    @task.sensor(
        poke_interval=30,
        timeout=300,
        mode="reschedule",
    )
    def verify_gold(config: Dict[str, str]) -> PokeReturnValue:
        db_name = os.environ.get("GOLD_DB_NAME", "gold")
        target_date = datetime.fromisoformat(config["target_date_str"]).date()
        date_key = int(target_date.strftime("%Y%m%d"))
        
        hook = GCSHook(gcp_conn_id="google_cloud_default")
        # Check for data files in the gold table location for the specific partition
        prefix = f"iceberg_warehouse/{db_name}.db/fact_sales/data/date_key={date_key}/"

        objects = hook.list(
            bucket_name=GOLD_BUCKET, # type: ignore
            prefix=prefix,
            max_results=1,
        )
        exists = bool(objects)
        
        if exists:
             return PokeReturnValue(is_done=True, xcom_value=config)
             
        return PokeReturnValue(is_done=False)

    @task(outlets=[gold_kimball_asset])
    def mark_gold_produced(config: Dict[str, str]):
        """Marks the gold asset as produced after Spark job success."""

        print(f"Gold Kimball fact table produced for date: {config['target_date_str']}")
        return Metadata(asset=gold_kimball_asset)

    waited = verify_gold(spark_job)  # type: ignore[arg-type]
    mark_gold_produced(waited)  # type: ignore[arg-type]


silver_to_gold_kimball()
