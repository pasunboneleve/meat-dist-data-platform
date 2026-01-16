import os
from datetime import UTC, date, datetime, timedelta
from typing import Any, Dict

from airflow.providers.google.cloud.operators.dataproc import \
    DataprocCreateBatchOperator
from airflow.providers.google.cloud.sensors.gcs import \
    GCSObjectsWithPrefixExistenceSensor
from airflow.sdk import dag, task

from assets import bronze_carcasses_asset, silver_dv2_asset

default_args = {
    "owner": "data-eng",
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    dag_id="bronze_to_silver_dv2",
    default_args=default_args,
    description="Bronze Parquet → Silver DV2 Iceberg via Dataproc Serverless Spark",
    schedule=[bronze_carcasses_asset],  # triggers when bronze asset is updated
    start_date=datetime(2024, 1, 1, tzinfo=UTC),
    catchup=False,
    tags=["transform", "silver", "dataproc", "iceberg"],
    max_active_runs=1,
)
def bronze_to_silver_dv2():
    @task
    def get_config(**context: Dict[str, Any]) -> Dict[str, str]:
        logical_date: date = context["logical_date"].date() - timedelta(days=1)  # type: ignore[arg-type]
        target_date_str = logical_date.isoformat()
        prefix = (
            f"carcasses/year={logical_date.year}/"
            f"month={logical_date.month}/"
            f"day={logical_date.day}/"
        )
        return {
            "target_date_str": target_date_str,
            "target_prefix": prefix,
        }

    @task
    def submit_spark_transform(config: Dict[str, str]):
        # Return the operator instance (TaskFlow will .execute() it)
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
                        "spark.sql.catalog.spark_catalog.warehouse": f"gs://{os.environ['SILVER_BUCKET']}/iceberg_warehouse/",
                        "spark.executor.instances": "2",
                        "spark.executor.cores": "4",
                        "spark.executor.memory": "4g",
                        "spark.driver.cores": "4",
                        "spark.driver.memory": "4g",
                        "spark.dynamicAllocation.enabled": "false",
                        "spark.sql.execution_date": "{{ ds }}",
                        "spark.sql.bronze_bucket": os.environ["BRONZE_BUCKET"],
                        "spark.sql.silver_bucket": os.environ["SILVER_BUCKET"],
                        "spark.sql.target_date_str": config[
                            "target_date_str"
                        ],  # ← from upstream
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
        from airflow.sdk import \
            get_current_context  # safe import inside task if needed

        sensor = GCSObjectsWithPrefixExistenceSensor(
            task_id="verify_silver_tables",
            bucket=SILVER_BUCKET,  # type: ignore[arg-type]
            prefix="iceberg_warehouse/default/hub_carcass/metadata/",  # adjust if dynamic
            google_cloud_conn_id="google_cloud_default",
            timeout=300,
            poke_interval=30,
            mode="reschedule",  # free worker while polling
            # deferrable=True,  # enable if your Airflow supports deferrable operators
        )

        context = get_current_context()
        sensor.execute(context=context)

        return config  # optional pass-through

    # Chain + produce silver asset on final success
    config = get_config()
    spark_job = submit_spark_transform(config)  # type: ignore[arg-type]
    waited = verify_silver(spark_job)  # type: ignore[arg-type]

    @task(outlets=[silver_dv2_asset])
    def mark_silver_produced(config: Dict[str, str]):
        print(
            f"Silver DV2 Iceberg asset produced for date: {config['target_date_str']}"
        )
        # Optional: add lightweight checks, notifications, etc.

    mark_silver_produced(waited)  # type: ignore[arg-type]


bronze_to_silver_dv2()
