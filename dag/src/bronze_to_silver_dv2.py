import os
from datetime import UTC, date, datetime, timedelta
from typing import Any, Dict

from airflow.datasets import Dataset
from airflow.decorators import asset, dag
from airflow.providers.google.cloud.operators.dataproc import \
    DataprocCreateBatchOperator
from airflow.providers.google.cloud.sensors.gcs import \
    GCSObjectsWithPrefixExistenceSensor

default_args = {
    "owner": "data-eng",
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}

bronze_carcasses_dataset = Dataset(f"gcs://{os.environ.get('BRONZE_BUCKET')}/carcasses")
silver_dv2_dataset = Dataset(f"gcs://{os.environ.get('SILVER_BUCKET')}/iceberg_warehouse")


@dag(
    dag_id="bronze_to_silver_dv2",
    default_args=default_args,
    description="Bronze Parquet to Silver DV2 Iceberg transform via Dataproc Serverless Spark",
    schedule=[bronze_carcasses_dataset],
    start_date=datetime(2024, 1, 1, tzinfo=UTC),
    catchup=False,
    tags=["transform", "silver", "dataproc", "iceberg"],
)
def bronze_to_silver_dv2_dag():
    @asset
    def get_config(**context: Dict[str, Any]) -> Dict[str, str]:
        """Load config from Airflow Variables."""
        logical_date: date = context["logical_date"].date() - timedelta(days=1)  # type: ignore
        target_date_str = logical_date.isoformat()
        prefix = f"carcasses/year={logical_date.year}/month={logical_date.month}/day={logical_date.day}/"
        return {
            "target_date_str": target_date_str,
            "target_prefix": prefix,
        }

    @asset
    def spark_transform(config: Dict[str, Any]):
        """Submits the Bronze to Silver Spark job."""
        return DataprocCreateBatchOperator(
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
                        "spark.sql.target_date_str": "{{ task_instance.xcom_pull(task_ids='get_config', key='return_value')['target_date_str'] }}",
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

    @asset(outlets=[silver_dv2_dataset])
    def verify_silver(spark_transform: None):
        """Checks for the creation of Silver Iceberg table metadata."""
        return GCSObjectsWithPrefixExistenceSensor(
            task_id="verify_silver_tables",
            bucket=os.environ["SILVER_BUCKET"],
            prefix="iceberg_warehouse/default/hub_carcass/metadata/",
            google_cloud_conn_id="google_cloud_default",
            timeout=300,
            poke_interval=30,
        )



bronze_to_silver_dv2_dag()
