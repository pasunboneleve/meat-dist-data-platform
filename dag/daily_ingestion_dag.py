from airflow import DAG
from airflow.providers.http.operators.http import HttpOperator
from airflow.providers.google.cloud.sensors.gcs import GCSObjectSensor
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "data-eng",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    dag_id="daily_synthetic_ingestion",
    default_args=default_args,
    description="Daily trigger for synthetic meat ingestion to Bronze GCS",
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["ingestion", "bronze"],
)

trigger_ingestion = HttpOperator(
    task_id="trigger_synthetic_ingestor",
    method="POST",
    endpoint="/",
    url="{{ var.value.ingestor_url }}",
    dag=dag,
)

wait_bronze = GCSObjectSensor(
    task_id="wait_for_bronze_data",
    bucket="{{ var.value.bronze_bucket }}",
    prefix="carcasses/year={{ ds_nodash[:4] }}/month={{ ds_nodash[4:6] }}/day={{ ds_nodash[6:8] }}/",
    google_cloud_conn_id="google_cloud_default",
    timeout=7200,
    poke_interval=600,
    dag=dag,
)

end = EmptyOperator(
    task_id="end",
    dag=dag,
)

trigger_ingestion >> wait_bronze >> end
