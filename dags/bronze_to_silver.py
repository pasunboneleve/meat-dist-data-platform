"""
Bronze to Silver DAG for Cloud Composer.
- Sensor for new daily bronze data.
- Submit Dataproc Serverless PySpark job.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocServerlessSparkSubmitOperator,
)
from airflow.providers.google.cloud.sensors.gcs import GCSObjectSensor
from airflow.operators.empty import EmptyOperator

# Dynamic config from Airflow Variables (set in UI)
PROJECT_ID = Variable.get("gcp_project", default_var="your-project-id")
REGION = Variable.get("gcp_region", default_var="australia-southeast2")
BRONZE_BUCKET = Variable.get("bronze_bucket", default_var=f"{PROJECT_ID}-bronze")
DEPS_BUCKET = Variable.get("deps_bucket", default_var=f"{PROJECT_ID}-deps")

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
    bucket="{{ var.value.bronze_bucket }}",
    prefix="carcasses/year={{ ds_nodash[:4] }}/month={{ ds_nodash[4:6] }}/day={{ ds_nodash[6:8] }}/",
    google_cloud_conn_id="google_cloud_default",
    timeout=7200,  # 2h
    poke_interval=600,  # 10min
    dag=dag,
)

submit_spark = DataprocServerlessSparkSubmitOperator(
    task_id="transform_bronze_to_silver",
    project_id="{{ var.value.gcp_project }}",
    region="{{ var.value.gcp_region }}",
    batch_id="bronze-to-silver-{{ ds_nodash }}",  # Unique ID
    main="job.py",
    script_uri="{{ var.value.deps_bucket }}/job.py",
    args=[
        "gs://{{ var.value.bronze_bucket }}/carcasses/year={{ ds_nodash[:4] }}/month={{ ds_nodash[4:6] }}/day={{ ds_nodash[6:8] }}/*"
    ],
    dag=dag,
)

end = EmptyOperator(task_id="end", dag=dag)

start >> wait_bronze >> submit_spark >> end
