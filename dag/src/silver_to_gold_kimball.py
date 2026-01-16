import os
from datetime import UTC, date, datetime, timedelta
from typing import Any, Dict

from airflow import DAG
from airflow.models.dataset import Dataset
from airflow.providers.google.cloud.operators.dataproc import \
    DataprocCreateBatchOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import task

default_args = {
    "owner": "data-eng",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

silver_dv2_dataset = Dataset(f"gcs://{os.environ.get('SILVER_BUCKET')}/iceberg_warehouse")
gold_kimball_dataset = Dataset(
    f"gcs://{os.environ.get('GOLD_BUCKET')}/fact_carcass_transactions"
)

dag = DAG(
    dag_id="silver_to_gold_kimball",
    default_args=default_args,
    description="Silver Iceberg to Gold Kimball transform via Dataproc Serverless Spark",
    schedule=[silver_dv2_dataset],
    start_date=datetime(2024, 1, 1, tzinfo=UTC),
    catchup=False,
    tags=["transform", "gold", "dataproc", "iceberg", "kimball"],
)


@task(dag=dag)
def get_config(**context: Dict[str, Any]) -> Dict[str, str]:
    """Load config from Airflow Variables."""
    logical_date: date = context["logical_date"].date() - timedelta(days=1)  # type: ignore
    target_date_str = logical_date.isoformat()
    return {
        "target_date_str": target_date_str,
    }


config = get_config()

start = EmptyOperator(task_id="start", dag=dag)

# Task for Spark transform
spark_transform_gold = DataprocCreateBatchOperator(
    task_id="transform_kimball_gold",
    project_id=f"{os.environ['GCP_PROJECT_ID']}",
    region=f"{os.environ['DATAPROC_REGION']}",
    batch_id="silver-to-gold-{{ ds_nodash }}-{{ ts_nodash | replace('T', '') | replace('+', '-') | lower }}-{{ ti.try_number }}",
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
                "spark.sql.target_date_str": "{{ task_instance.xcom_pull(task_ids='get_config', key='return_value')['target_date_str'] }}",
            },
        },
        "environment_config": {
            "execution_config": {
                "service_account": f"{os.environ['DATAPROC_BATCH_SERVICE_ACCOUNT']}",
            }
        },
    },
    gcp_conn_id="google_cloud_default",
    outlets=[gold_kimball_dataset],
)

end = EmptyOperator(task_id="end", dag=dag)

config >> start >> spark_transform_gold >> end
