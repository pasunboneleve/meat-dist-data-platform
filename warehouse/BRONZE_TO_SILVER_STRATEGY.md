# Bronze to Silver Pipeline: Implementation Strategy

## Goal
Implement a cost-effective (~$1–5/run), serverless orchestration pipeline using **Cloud Composer (Airflow 2.9+)** to:
1. **Ingest** synthetic meat carcass data to **Bronze GCS** (partitioned Parquet, already partially set up).
2. **Transform** Bronze → **Silver** (Data Vault 2.0 Iceberg tables in GCS).
3. Auto-discover via **Dataplex** for governance/querying.
4. **Modern & Simple**: Airflow DAGs + Dataproc Serverless PySpark (no clusters, pay-per-use).
5. **Low Cost**: Serverless (no idle fees), small jobs (1–2 vCPU, 30min max), daily schedule.

## Key Principles
- **DataPlex**: Auto-discovery, metadata management, lineage tracking.
- **Idempotency**: Iceberg MERGE-into for safe retries/replays (no duplicates).
- **Medallion Architecture**: Bronze (raw Parquet), Silver (Data Vault 2.0), Gold (Kimball star schema).
- **BigLake**: Serverless BigQuery querying over Iceberg tables (no duplication).
- **Zero Egress Fees**: All resources/services in single region (e.g., australia-southeast1).

**Total Monthly Cost Est.**: <$20 (Composer idle ~$50, but jobs ~$0.10–0.50 each).

## Architecture Flow
```
Cloud Scheduler (daily) → Cloud Run (synthetic generator) → Bronze GCS (Parquet: plant_id/year/month/day)
  ↓ (Airflow DAG trigger/verify)
Composer DAG:
  - Task 1: Verify/Trigger new Bronze data
  - Task 2: Dataproc Serverless Spark → Read Bronze Parquet → DV2 Iceberg → Silver GCS (table_version=1, partitioned)
Dataplex: Auto-discovers → BigLake/BigQuery queryable
```

## Key Technologies (Modern/Simple)
- **Orchestration**: Cloud Composer 3 (Airflow) – DAGs for scheduling/monitoring/retries.
- **Ingestion**: Existing `synthetic-meat` Cloud Run (HTTP trigger) + GCS.
- **Transform**: Dataproc Serverless Spark 2.1+ (PySpark) w/ **Apache Iceberg** (MERGE/CDC support).
- **Catalog**: **Iceberg REST Catalog** (simple GCS-backed, no Hive/DB needed) or **Nessie** (free, Git-like).
- **No extras**: Avoid KubernetesOperator, custom plugins – use built-in `DataprocServerlessSparkBatchOperator` (Airflow 2.8+).
- **Testing**: Local Spark + pytest; Airflow TaskGroups for modularity.

## Phase 1: Ingestion DAG (1–2 hours)
**DAG**: `daily_ingestion_orchestrator` (schedule `@daily`, `start_date=now-1d`).

Tasks:
1. **HttpSensor** / **HttpOperator**: Trigger Cloud Run (`POST https://synthetic-meat-ingestor-...run.app/generate`).
2. **BigQueryCheckOperator** or **GCSHook**: Verify new Parquet files landed (`gs://bronze/carcasses/plant_id=.../year=...`).
3. **EmailOperator** (failures only).

**Why Airflow?** Replaces/backs up Cloud Scheduler; retries, SLAs, lineage.

**IaC**:
- Add `google_cloud_composer_environment_update`? No, upload DAGs to Composer GCS DAGs folder via CI/CD or Airflow UI.

## Phase 2: Bronze → Silver Transform (2–4 hours)
**DAG**: `bronze_to_silver_dv2` (triggered by ingestion DAG or `@daily`; `depends_on_past=True`).

**PySpark Job** (`transform_bronze_to_silver.py`):
- Read: `spark.read.parquet("gs://bronze/carcasses/*")` (dynamic partitions).
- Model **Data Vault 2.0** (simple Kimball hybrid):
  | Entity | BK | Attributes | Partition |
  |--------|----|------------|-----------|
  | `hub_carcass` | `carcass_id` | load_dts, rec_src | `load_date` |
  | `hub_plant` | `plant_id` | load_dts | - |
  | `sat_carcass_detail` | `carcass_hk` (hash) | weight_kg, grade, price/kg, marbling | `slaughter_date` |
  | `link_carcass_process` | `carcass_hk + plant_hk` | process_dts | `process_date` |
- Iceberg: `df.writeTo("silver.hub_carcass").using("iceberg").createOrReplace()` (MERGE for SCD).
- Config: REST Catalog (`iceberg.catalog.warehouse=gs://silver/tables`).

**Airflow Operator**: `DataprocServerlessSparkBatchOperator`
```python
DataprocServerlessSparkBatchOperator(
  task_id="transform_silver",
  project_id="{{ var.value.gcp_project }}",
  region="australia-southeast2",
  batch_id="bronze-to-silver-{{ ds_nodash }}",
  spark_batch={
    "jar_file_uris": ["gs://deps/iceberg-spark-runtime-1.6.1.jar"],
    "main_class": "NoMain",  # PySpark script
    "python_file_uris": ["gs://dags/transform_bronze_to_silver.py"],
    "runtime_config": {
      "version": "3.1-Debian10",
      "container_image": "gcr.io/dataproc-serverless/spark:3.1.1"
    },
    "spark_sql_job": {... args for script},
  },
  ...
)
```
- **Jars**: Pre-upload Iceberg runtime to `deps` bucket (Terraform).

**Why Dataproc Serverless?** ~$0.05/vCPU-hour, auto-scales, Iceberg native, no cluster mgmt.

## Phase 3: IaC & CI/CD Updates (1 hour)
**Terraform (warehouse/)**:
- Upload Iceberg JARs to `deps` bucket (`null_resource` w/ local-exec gsutil cp).
- Composer DAG folder policy for GitHub Actions SA.
- Output Composer Airflow URI/GCS DAGs path.

**DAG Upload**:
- GH Actions: On merge to `warehouse/dags/`, `gsutil rsync dags/ gs://meat-composer-dags/dags/`.

**Security**: Use Airflow Connections/Variables for GCS buckets, secrets.

## Phase 4: Testing & Monitoring (1 hour)
- **Local**: `spark-submit --packages org.apache.iceberg:iceberg-spark-runtime:1.6.1 ...` w/ GCS creds.
- **Unit**: Pytest on synthetic data samples.
- **Airflow**: Test DAG runs, XCom for lineage.
- **Monitor**: Composer logs, Dataplex lineage, BigQuery dry-run queries.
- **Cost Alerts**: Budget ~$100/month.

## Next Steps (Use this as guide)
1. Create `warehouse/dags/` dir + sample DAG YAML/Python.
2. Add Iceberg JAR to deps (Terraform).
3. Implement PySpark script in `warehouse/spark_jobs/`.
4. Test end-to-end: Trigger DAG → Query Silver via BigQuery (`SELECT * FROM biglake.silver.hub_carcass`).
5. Gold layer: Simple BQ views (next phase).

**Risks/Mitigations**:
- Iceberg Catalog: Start w/ Hadoop catalog (GCS file), upgrade to REST.
- Partitions: Dynamic overwrite/append.
- Scale: Serverless handles 1M+ rows/batch.

This is **serverless-first, IaC-driven, observable**—perfect for portfolio/learning.
