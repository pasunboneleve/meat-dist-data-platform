import os
from datetime import UTC, datetime, timedelta
from typing import Any, Dict

from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCheckOperator
from airflow.providers.google.cloud.operators.dataproc_serverless import (
    DataprocServerlessSparkBatchOperator,
)
from airflow.providers.google.cloud.sensors.gcs import GCSObjectsWithPrefixExistenceSensor

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
    project_id = Variable.get("gcp_project_id")
    region = Variable.get("dataproc_region", default_var="australia-southeast1")
    bronze_bucket = Variable.get("BRONZE_BUCKET")
    silver_bucket = Variable.get("SILVER_BUCKET")
    deps_bucket = f"{bronze_bucket.replace('-bronze', '-deps')}"  # Assume deps bucket pattern
    logical_date = context["logical_date"].date()
    target_date_str = logical_date.strftime("%Y/%m/%d")
    prefix = f"carcasses/**/year={logical_date.year}/month={logical_date.month:02d}/day={logical_date.day:02d}/"
    return {
        "project_id": project_id,
        "region": region,
        "bronze_bucket": bronze_bucket,
        "silver_bucket": silver_bucket,
        "deps_bucket": deps_bucket,
        "target_prefix": prefix,
        "target_date": target_date_str,
    }

config = get_config()

# Task 1: Verify new Bronze files for yesterday
verify_bronze = GCSObjectsWithPrefixExistenceSensor(
    task_id="verify_new_bronze",
    bucket="{{ ti.xcom_pull(task_ids='get_config')['bronze_bucket'] }}",
    prefix="{{ ti.xcom_pull(task_ids='get_config')['target_prefix'] }}",
    google_cloud_conn_id="google_cloud_default",
    timeout=3600,
    poke_interval=300,
    dag=dag,
)

# Task 2: Spark transform
spark_transform = DataprocServerlessSparkBatchOperator(
    task_id="transform_dv2_iceberg",
    project_id="{{ ti.xcom_pull(task_ids='get_config')['project_id'] }}",
    region="{{ ti.xcom_pull(task_ids='get_config')['region'] }}",
    batch_id=f"bronze-to-silver-dv2-{{{{ ds_nodash }}}}",
    spark_batch={
        "jar_file_uris": [
            "gs://{{ ti.xcom_pull(task_ids='get_config')['deps_bucket'] }}/iceberg-spark-runtime-1.6.1_3.5.0.jar",
        ],
        "python_file_uris": [
            "gs://{{ var.value.composer_bucket }}/dags/transform_bronze_to_silver.py",  # Synced by CI/CD
        ],
        "file_uris": [],
        "main_python_file_uri": "gs://{{ var.value.composer_bucket }}/dags/transform_bronze_to_silver.py",
        "runtime_config": {
            "version": "3.5-Debian12",
            "container_image": "gcr.io/dataproc-serverless/spark:3.5.0",
            "properties": {
                "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
                "spark.sql.catalog.spark_catalog": "org.apache.iceberg.spark.SparkSessionCatalog",
                "spark.sql.catalog.spark_catalog.type": "hadoop",
                "spark.sql.catalog.iceberg": "org.apache.iceberg.spark.SparkCatalog",
                "spark.sql.catalog.iceberg.type": "hadoop",
                "spark.sql.catalog.iceberg.warehouse": "gs://{{ ti.xcom_pull(task_ids='get_config')['silver_bucket'] }}/tables",
            }
        },
    },
    dag=dag,
)

# Task 3: Verify Silver tables updated via BigLake query
verify_silver = BigQueryCheckOperator(
    task_id="verify_silver_tables",
    sql="""
    SELECT COUNT(*) > 0
    FROM `{{ ti.xcom_pull(task_ids='get_config')['project_id'] }}.meat_market_lake.curated_zone.hub_carcass`
    """,
    use_legacy_sql=False,
    bigquery_conn_id="google_cloud_default",
    dag=dag,
)

end = EmptyOperator(task_id="end", dag=dag)

config >> verify_bronze >> spark_transform >> verify_silver >> end
