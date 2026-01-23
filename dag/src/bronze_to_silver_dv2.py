import os
from datetime import UTC, datetime, timedelta
from typing import Dict

from airflow.models.taskinstance import TaskInstance
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.operators.dataproc import \
    DataprocCreateBatchOperator
from airflow.sdk import PokeReturnValue, dag, task
from airflow.sdk.definitions.asset.metadata import Metadata

from utils.assets import bronze_carcasses_asset, silver_dv2_asset
from utils.config import generate_dataproc_batch_id, get_config_from_trigger

default_args = {
    "owner": "data-eng",
    "retries": 2,
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
        return get_config_from_trigger(
            context=context,
            upstream_asset=bronze_carcasses_asset,
        )

    config_task = get_config()

    submit_spark_transform = DataprocCreateBatchOperator(
        task_id="submit_spark_transform",
        project_id=os.environ["GCP_PROJECT_ID"],
        region=os.environ["DATAPROC_REGION"],
        batch_id="{{ task.env['utils.config'].generate_dataproc_batch_id(prefix='bronze-to-silver-dv2') }}",
        batch={
            "pyspark_batch": {
                "main_python_file_uri": f"gs://{os.environ['DEPS_BUCKET']}/spark_jobs/transform_bronze_to_silver.py",
            },
            "runtime_config": {
                "version": "2.2",
                "properties": {
                    "spark.jars.packages": "org.apache.iceberg:iceberg-spark-runtime-3.5_2.13:1.10.1,org.apache.iceberg:iceberg-gcp-bundle:1.10.1",
                    "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
                    "spark.sql.iceberg.merge-schema": "true",
                    "spark.executor.instances": "2",
                    "spark.executor.cores": "4",
                    "spark.executor.memory": "4g",
                    "spark.driver.cores": "4",
                    "spark.driver.memory": "4g",
                    "spark.dynamicAllocation.enabled": "false",
                    "spark.sql.execution_date": "{{ ds }}",
                    "spark.sql.bronze_bucket": BRONZE_BUCKET,
                    "spark.sql.silver_bucket": SILVER_BUCKET,
                    "spark.sql.target_date_str": "{{ ti.xcom_pull(task_ids='get_config')['target_date_str'] }}",
                    "spark.sql.gcp_project": os.environ["GCP_PROJECT_ID"],
                    "spark.sql.dataproc_region": os.environ["DATAPROC_REGION"],
                    "spark.sql.catalog_name": os.environ["CATALOG_NAME"],
                    "spark.sql.db_name": os.environ["DB_NAME"],
                },
            },
            "environment_config": {
                "execution_config": {
                    "service_account": os.environ["DATAPROC_BATCH_SERVICE_ACCOUNT"],
                }
            },
            "labels": {
                "layer": "bronze-to-silver",
            },
        },
        gcp_conn_id="google_cloud_default",
    )

    @task.sensor(
        poke_interval=30,
        timeout=300,
        mode="reschedule",
    )
    def verify_silver(config: Dict[str, str]) -> PokeReturnValue:
        db_name = os.environ.get("DB_NAME")
        if not db_name:
            raise ValueError("Missing DB_NAME env var")

        hook = GCSHook(gcp_conn_id="google_cloud_default")
        prefix = f"iceberg_warehouse/{db_name}/hub_carcass/metadata/"

        objects = hook.list(
            bucket_name=SILVER_BUCKET,  # type: ignore
            prefix=prefix,
            max_results=1,
        )
        exists = bool(objects)

        if exists:
            return PokeReturnValue(is_done=True, xcom_value=config)

        return PokeReturnValue(is_done=False)

    @task(outlets=[silver_dv2_asset])
    def mark_asset_produced(config: Dict[str, str], **context):
        print(
            f"Silver DV2 Iceberg asset produced for date: {config['target_date_str']}"
        )
        ti: TaskInstance = context["ti"]
        ti.xcom_push(key="asset_config", value=config)
        return Metadata(asset=silver_dv2_asset)

    waited = verify_silver(config=config_task)  # type: ignore
    marked = mark_asset_produced(config=waited)  # type: ignore

    config_task >> submit_spark_transform >> waited


bronze_to_silver_dv2()
