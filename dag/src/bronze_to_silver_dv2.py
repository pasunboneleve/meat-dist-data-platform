import os
from datetime import UTC, datetime, timedelta
from typing import Dict

from airflow.providers.google.cloud.operators.dataproc import \
    DataprocCreateBatchOperator
from airflow.providers.google.cloud.sensors.gcs import \
    GCSObjectsWithPrefixExistenceSensor
from airflow.sdk import dag, get_current_context, task

from asset_utils import emit_asset_with_metadata
from assets import bronze_carcasses_asset, silver_dv2_asset
from config_utils import generate_dataproc_batch_id, get_target_config

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
        return get_target_config(
            context=context,
            upstream_asset=bronze_carcasses_asset,
            date_offset_days=1,  # change if different DAG needs different offset
            metadata_key="target_date_str",  # change if upstream uses different key
        )

    @task
    def submit_spark_transform(config: Dict[str, str]):
        batch_id = generate_dataproc_batch_id(
            prefix="bronze-to-silver-dv2",
        )
        operator = DataprocCreateBatchOperator(
            task_id="transform_dv2_iceberg",
            project_id=os.environ["GCP_PROJECT_ID"],
            region=os.environ["DATAPROC_REGION"],
            batch_id=batch_id,
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
        context = get_current_context()
        return operator.execute(context=context)

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

    @task(outlets=[silver_dv2_asset])
    def mark_asset_produced(config: Dict[str, str], **context):
        print(
            f"Silver DV2 Iceberg asset produced for date: {config['target_date_str']}"
        )

        emit_asset_with_metadata(
            context=context,
            asset=silver_dv2_asset,
            extra={
                "target_date_str": config["target_date_str"],
                "target_prefix": config.get("target_prefix"),
            },
            log_prefix="Emitted silver_dv2_asset",
        )

    # Chain
    config = get_config()
    spark_job = submit_spark_transform(config)  # type: ignore[arg-type]
    waited = verify_silver(spark_job)  # type: ignore[arg-type]
    mark_asset_produced(waited)  # type: ignore[arg-type]


bronze_to_silver_dv2()
