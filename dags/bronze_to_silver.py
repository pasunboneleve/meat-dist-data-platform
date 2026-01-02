"""
Bronze to Silver DAG for Cloud Composer.
- Sensor for new daily bronze data.
- Submit Dataproc Serverless PySpark job.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocServerlessSparkSubmitOperator,
)
from airflow.providers.google.cloud.sensors.gcs import GCSObjectSensor
from airflow.operators.empty import EmptyOperator

PROJECT_ID = "your-project-id"  # Set via Airflow Variable 'gcp_project'
REGION = "australia-southeast2"
BRONZE_BUCKET = f"{PROJECT_ID}-bronze"
DEPS_BUCKET = f"{PROJECT_ID}-deps"

default_args = {
    "owner": "data-eng",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    dag_id="bronze_to_silver",
    default_args=default_args,
    description="Ingest bronze Parquet to silver Iceberg DV2",
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["dataproc", "iceberg", "dv2"],
    max_active_runs=1,
)

start = EmptyOperator(task_id="start", dag=dag)

wait_bronze = GCSObjectSensor(
    task_id="wait_for_bronze_data",
    bucket=BRONZE_BUCKET,
    prefix=f"carcasses/year={{{{ ds_nodash[:4] }}}}/month={{{{ ds_nodash[4:6] }}}}/day={{{{ ds_nodash[6:8] }}}}/",
    google_cloud_conn_id="google_cloud_default",
    timeout=7200,  # 2h
    poke_interval=600,  # 10min
    dag=dag,
)

submit_spark = DataprocServerlessSparkSubmitOperator(
    task_id="transform_bronze_to_silver",
    project_id=PROJECT_ID,
    region=REGION,
    batch_id=f"bronze-to-silver-{{{{ ds_nodash }}}}",  # Unique ID
    main="job.py",
    script_uri=f"gs://{DEPS_BUCKET}/job.py",
    # Add jars if needed
    spark_jars=[
        "gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar",  # Optional
    ],
    # Pass args: bronze path for the day
    args=[f"gs://{BRONZE_BUCKET}/carcasses/year={{{{ ds_nodash[:4] }}}}/month={{{{ ds_nodash[4:6] }}}}/day={{{{ ds_nodash[6:8] }}}}/*"],
    dag=dag,
)

end = EmptyOperator(task_id="end", dag=dag)

start >> wait_bronze >> submit_spark >> end
