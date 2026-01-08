import os
from datetime import UTC, date, datetime, timedelta
from typing import Any, Dict

from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import \
    BigQueryCheckOperator
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
    project_id = os.environ["GCP_PROJECT_ID"]
    region = os.environ["DATAPROC_REGION"]
    bronze_bucket = os.environ["BRONZE_BUCKET"]
    silver_bucket = os.environ["SILVER_BUCKET"]
    deps_bucket = os.environ["DEPS_BUCKET"]
    logical_date: date = context["logical_date"].date() - timedelta(days=1)  # type: ignore
    target_date_str = logical_date.strftime("%Y/%m/%d")
    prefix = f"carcasses/year={logical_date.year}/month={logical_date.month}/day={logical_date.day}/"
    return {
        "GCP_PROJECT_ID": project_id,
        "DATAPROC_REGION": region,
        "BRONZE_BUCKET": bronze_bucket,
        "SILVER_BUCKET": silver_bucket,
        "DEPS_BUCKET": deps_bucket,
        "target_prefix": prefix,
        "target_date": target_date_str,
    }


config = get_config()

# Task 1: Verify new Bronze files for yesterday
verify_bronze = GCSObjectsWithPrefixExistenceSensor(
    task_id="verify_new_bronze",
    bucket="{{ ti.xcom_pull(task_ids='get_config')['BRONZE_BUCKET'] }}",
    prefix="{{ ti.xcom_pull(task_ids='get_config')['target_prefix'] }}",
    google_cloud_conn_id="google_cloud_default",
    timeout=300,  # Short for testing (5min)
    poke_interval=60,  # Poke every 1min
    dag=dag,
)

import os

# Task 2: Spark transform
spark_transform = DataprocCreateBatchOperator(
    task_id="transform_dv2_iceberg",
    project_id="{{ ti.xcom_pull(task_ids='get_config')['GCP_PROJECT_ID'] }}",
    region="{{ ti.xcom_pull(task_ids='get_config')['DATAPROC_REGION'] }}",
    batch_id="bronze-to-silver-dv2-{{ ds_nodash }}-{{ ti.try_number }}",
    batch={
        "pyspark_batch": {
            "main_python_file_uri": "gs://{{ ti.xcom_pull(task_ids='get_config')['DEPS_BUCKET'] }}/spark_jobs/transform_bronze_to_silver.py",
            "args": [
                "--execution-date={{ ds }}",
                "--bronze-bucket={{ ti.xcom_pull(task_ids='get_config')['BRONZE_BUCKET'] }}",
                "--silver-bucket={{ ti.xcom_pull(task_ids='get_config')['SILVER_BUCKET'] }}",
            ],
            "python_file_uris": [],  # Correctly nested and safe (empty is fine)
            "jar_file_uris": [
                "gs://spark-lib/iceberg/iceberg-spark-runtime-3.5_2.12-1.5.2.jar",
            ],
        },
        "runtime_config": {
            "version": "3.0",  # Valid: latest GA Serverless runtime (recommended)
            # "2.2" is also excellent if you prefer the LTS default
            "properties": {
                "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
                "spark.sql.catalog.spark_catalog": "org.apache.iceberg.spark.SparkCatalog",  # Correct class for REST
                "spark.sql.catalog.spark_catalog.type": "rest",
                "spark.sql.catalog.spark_catalog.uri": "https://biglake.googleapis.com/iceberg/v1beta/restcatalog",
                "spark.sql.catalog.spark_catalog.warehouse": "gs://{{ ti.xcom_pull(task_ids='get_config')['SILVER_BUCKET'] }}/iceberg_warehouse/",
            },
        },
        "labels": {
            "dag_id": "bronze-to-silver-dv2",
            "date": "{{ ds_nodash }}",
        },
    },
    gcp_conn_id="google_cloud_default",
)
# Task 3: Verify Silver tables updated via BigLake query
verify_silver = BigQueryCheckOperator(
    task_id="verify_silver_tables",
    sql="""
    SELECT COUNT(*) > 0
    FROM `{{ ti.xcom_pull(task_ids='get_config')['GCP_PROJECT_ID'] }}.meat_market_lake.curated_zone.hub_carcass`
    """,
    use_legacy_sql=False,
    gcp_conn_id="google_cloud_default",
    dag=dag,
)

end = EmptyOperator(task_id="end", dag=dag)

config >> verify_bronze >> spark_transform >> verify_silver >> end
