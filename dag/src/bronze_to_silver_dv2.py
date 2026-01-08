import os
from datetime import UTC, date, datetime, timedelta
from typing import Any, Dict

from airflow import DAG
from airflow.models import Variable
from airflow.providers.google.cloud.operators.bigquery import \
    BigQueryCheckOperator
from airflow.providers.google.cloud.operators.dataproc import \
    DataprocCreateBatchOperator
from airflow.providers.google.cloud.sensors.gcs import \
    GCSObjectsWithPrefixExistenceSensor
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import DAG, Variable, task

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
    prefix = f"carcasses/year={logical_date.year}/month={logical_date.month:02d}/day={logical_date.day:02d}/"
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
    timeout=300,  # Short for testing (5min)
    poke_interval=60,  # Poke every 1min
    dag=dag,
)

# Task 2: Spark transform
spark_transform = DataprocCreateBatchOperator(
    task_id="transform_dv2_iceberg",
    project_id="{{ ti.xcom_pull(task_ids='get_config')['project_id'] }}",
    region="{{ ti.xcom_pull(task_ids='get_config')['region'] }}",
    # Updated: Ensure batch_id is unique per execution to avoid 409 Conflicts
    batch_id=f"bronze-to-silver-dv2-{{ ds_nodash }}",
    batch={
        "spark_batch": {
            "jar_file_uris": [
                "gs://{{ ti.xcom_pull(task_ids='get_config')['deps_bucket'] }}/iceberg-spark-runtime-3.5_2.12-1.6.1.jar",
            ],
            "python_file_uris": [
                "gs://{{ ti.xcom_pull(task_ids='get_config')['deps_bucket'] }}/spark_jobs/transform_bronze_to_silver.py",
            ],
            "main_python_file_uri": "gs://{{ ti.xcom_pull(task_ids='get_config')['deps_bucket']}}/spark_jobs/transform_bronze_to_silver.py",
            "runtime_config": {
                "version": "3.5-Debian12",
                "container_image": "gcr.io/dataproc-serverless/spark:3.5.0",
                "properties": {
                    "spark.sql.project_id": "{{ ti.xcom_pull(task_ids='get_config')['project_id'] }}",
                    "spark.sql.bronze_bucket": "{{ ti.xcom_pull(task_ids='get_config')['bronze_bucket'] }}",
                    "spark.sql.silver_bucket": "{{ ti.xcom_pull(task_ids='get_config')['silver_bucket'] }}",
                    "spark.sql.target_date": "{{ ti.xcom_pull(task_ids='get_config')['target_date'] }}",
                    "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
                    "spark.sql.catalog.spark_catalog": "org.apache.iceberg.spark.SparkSessionCatalog",
                    "spark.sql.catalog.spark_catalog.type": "hadoop",
                    "spark.sql.catalog.iceberg": "org.apache.iceberg.spark.SparkCatalog",
                    "spark.sql.catalog.iceberg.type": "hadoop",
                    "spark.sql.catalog.iceberg.warehouse": "gs://{{ ti.xcom_pull(task_ids='get_config')['silver_bucket'] }}/tables",
                },
            },
        },
    },
)


# Task 3: Verify Silver tables updated via BigLake query
verify_silver = BigQueryCheckOperator(
    task_id="verify_silver_tables",
    sql="""
    SELECT COUNT(*) > 0
    FROM `{{ ti.xcom_pull(task_ids='get_config')['project_id'] }}.meat_market_lake.curated_zone.hub_carcass`
    """,
    use_legacy_sql=False,
    gcp_conn_id="google_cloud_default",
    dag=dag,
)

end = EmptyOperator(task_id="end", dag=dag)

config >> verify_bronze >> spark_transform >> verify_silver >> end
