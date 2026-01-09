import hashlib
import os
from datetime import UTC, date, datetime, timedelta
from typing import Any, Dict

from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import \
    DataprocCreateBatchOperator
from airflow.providers.google.cloud.sensors.gcs import \
    GCSObjectsWithPrefixExistenceSensor
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import DAG, task

default_args = {
    "owner": "data-eng",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    dag_id="bronze_to_silver_dv2",
    default_args=default_args,
    description="Bronze Parquet to Silver DV2 Iceberg transform via Dataproc Serverless Spark",
    schedule="@daily",
    start_date=datetime(2024, 1, 1, tzinfo=UTC),
    catchup=False,
    tags=["transform", "silver", "dataproc", "iceberg"],
)


@task(dag=dag)
def get_config(**context: Dict[str, Any]) -> Dict[str, str]:
    """Load config from Airflow Variables."""
    logical_date: date = context["logical_date"].date() - timedelta(days=1)  # type: ignore
    target_date_str = logical_date.isoformat()
    prefix = f"carcasses/year={logical_date.year}/month={logical_date.month}/day={logical_date.day}/"
    return {
        "target_date": target_date_str,
        "target_prefix": prefix,
    }


config = get_config()

# Task 1: Verify new Bronze files for yesterday
verify_bronze = GCSObjectsWithPrefixExistenceSensor(
    task_id="verify_new_bronze",
    bucket=os.environ["BRONZE_BUCKET"],
    prefix="{{ ti.xcom_pull(task_ids='get_config')['target_prefix'] }}",
    google_cloud_conn_id="google_cloud_default",
    timeout=300,  # Short for testing (5min)
    poke_interval=60,  # Poke every 1min
    dag=dag,
)

# Task 2: Spark transform
spark_transform = DataprocCreateBatchOperator(
    task_id="transform_dv2_iceberg",
    project_id=f"{os.environ['GCP_PROJECT_ID']}",
    region=f"{os.environ['DATAPROC_REGION']}",
    batch_id="bronze-to-silver-dv2-{{ ds_nodash }}-{{ ts_nodash | replace('T', '') | replace('+', '-') | lower }}-{{ ti.try_number }}",
    batch={
        "pyspark_batch": {
            "main_python_file_uri": f"gs://{os.environ['DEPS_BUCKET']}/spark_jobs/transform_bronze_to_silver.py",
            "jar_file_uris": [
                f"gs://{os.environ['DEPS_BUCKET']}/iceberg-spark-runtime-3.5_2.13-1.10.1.jar",
            ],
        },
        "runtime_config": {
            # "version": "3.0",  # latest GA Serverless (requires auth_config in tf)
            "version": "2.2",  # is also excellent if you prefer the LTS default
            "properties": {
                "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
                "spark.sql.catalog.spark_catalog": "org.apache.iceberg.spark.SparkCatalog",  # Correct class for REST
                "spark.sql.catalog.spark_catalog.type": "rest",
                "spark.sql.catalog.spark_catalog.uri": "https://biglake.googleapis.com/iceberg/v1beta/restcatalog",
                "spark.sql.catalog.spark_catalog.warehouse": f"gs://{os.environ['SILVER_BUCKET']}/iceberg_warehouse/",
                # === Your templated config values ===
                "spark.sql.execution_date": "{{ ds }}",  # e.g., 2026-01-09
                "spark.sql.bronze_bucket": os.environ["BRONZE_BUCKET"],
                "spark.sql.silver_bucket": os.environ["SILVER_BUCKET"],
                "spark.sql.target_date": "{{ ti.xcom_pull(task_ids='get_config')['target_date_str'] }}",
            },
        },
        "environment_config": {
            "execution_config": {
                "service_account": f"{os.environ['DATAPROC_BATCH_SERVICE_ACCOUNT']}",
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


# Task 3: Verify Silver Iceberg metadata files created
verify_silver = GCSObjectsWithPrefixExistenceSensor(
    task_id="verify_silver_tables",
    bucket=os.environ["SILVER_BUCKET"],
    prefix="iceberg_warehouse/hub_carcass/metadata/",
    google_cloud_conn_id="google_cloud_default",
    timeout=300,
    poke_interval=30,
    dag=dag,
)

end = EmptyOperator(task_id="end", dag=dag)

config >> verify_bronze >> spark_transform >> verify_silver >> end
