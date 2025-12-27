# Modern Open Lakehouse Portfolio Project on Google Cloud

[![CI/CD Status](https://github.com/pasunboneleve/meat-dist-data-platform/actions/workflows/deploy.yml/badge.svg)](https://github.com/pasunboneleve/meat-dist-data-platform/actions/workflows/deploy.yml)

**Goal**: Build a cost-effective (~$10–30/month), serverless-first Lakehouse ingesting crypto trade data from Coinbase, demonstrating modern data engineering practices (Iceberg, DataPlex, Data Vault 2.0 + Kimball, Terraform IaC, CI/CD).

**Key Technologies**:
- **Data Source**: Coinbase Advanced Trade REST API (polling for recent trades – no API key needed).
- **Ingestion**: Cloud Functions (Python) triggered by Cloud Scheduler (every 1–5 minutes).
- **Bronze Layer**: Raw JSON/Parquet files in GCS.
- **Silver Layer**: Data Vault 2.0 modeled Iceberg tables in GCS.
- **Gold Layer**: Kimball star schema views or materialized tables queried via BigQuery (over Iceberg/BigLake).
- **Transformations**: Dataproc Serverless Spark (PySpark) batches for Iceberg support.
- **Catalog & Governance**: DataPlex Universal Catalog (auto-discovery, lineage).
- **BI**: Looker Studio (free) public dashboards.
- **IaC**: OpenTofu for everything, configured using HCL.
- **CI/CD & Testing**: GitHub Actions (lint, plan, tests, apply on merge).

## Project Structure (Suggested Git Repo Layout)

```
repo-root/
├── infra/                  # Terraform modules and configs
│   ├── main.tf
│   ├── variables.tf
│   ├── outputs.tf
│   └── modules/
├── functions/              # Cloud Functions code
│   └── ingestion/
│       ├── main.py
│       ├── requirements.txt
│       └── test_ingestion.py
├── spark_jobs/             # Dataproc Serverless scripts
│   ├── bronze_to_silver_dv.py
│   └── silver_to_gold_kimball.py
├── tests/                  # Integration/unit tests
├── .github/workflows/
│   └── ci-cd.yml           # GitHub Actions pipeline
└── README.md
```

## Phase 1: Project Setup (1–2 hours)

1. Create a new GCP project, enable billing.
2. Enable required APIs:
   - Cloud Functions API
   - Cloud Scheduler API
   - Cloud Build API
   - Dataproc API
   - BigQuery API
   - DataPlex API
   - Cloud Storage API
3. Install locally: `gcloud` CLI, Terraform, Git.
4. Create GitHub repo and clone locally.

## Phase 2: Infrastructure with Terraform

Deploy in this order:

- GCS buckets:
  - `${project_id}-bronze`
  - `${project_id}-silver`
  - `${project_id}-deps` (for Spark jars/temp)
- DataPlex Lake with zones:
  - Lake: `crypto-lake`
  - Zones: `raw` (bronze), `curated` (silver)
  - Assets linking buckets to zones
- BigQuery dataset: `gold_crypto`
- Service accounts & IAM:
  - One for Cloud Functions (Storage writer, DataPlex)
  - One for Dataproc (BigQuery, Storage, DataPlex roles)
- BigLake connection (if needed for Iceberg catalog)

Use community modules where possible (e.g., GoogleCloudPlatform/cloud-foundation-fabric).

Validate locally: `tofu init → fmt → validate → plan → apply`.

## Phase 3: Ingestion – Coinbase Polling to Bronze

- Endpoint: `GET https://api.coinbase.com/api/v3/brokerage/products/{product_id}/ticker` or `/trades` (paginated).
- Focus on 1–3 pairs initially (e.g., BTC-USD, ETH-USD).
- Cloud Function (2nd gen, Python 3.11+):
  - Track last ingested timestamp (store in GCS file or Firestore lite).
  - Fetch new trades, convert to Parquet (pandas + pyarrow).
  - Write partitioned: `gs://bronze/trades/product=BTC-USD/year=2025/month=12/day=27/trades.parquet`
- Trigger: Cloud Scheduler cron job (e.g., `*/5 * * * *` for every 5 min) → HTTP trigger on Function.
- DataPlex auto-discovers new files → BigLake external tables appear.

## Phase 4: Transformations

### Bronze → Silver (Data Vault 2.0 with Iceberg)

Use Dataproc Serverless PySpark batch:

- Catalog: BigLakeCatalog (integrated with DataPlex).
- Read bronze Parquet.
- Build DV2 entities:
  - Hub_Trade (business key: trade_id)
  - Hub_Product (product_id)
  - Sat_Trade_Details
  - Link_Trade_Product (if needed)
- Write as Iceberg tables in silver bucket, partitioned appropriately.
- Trigger initially manual (`gcloud dataproc batches submit`), later via Scheduler or Pub/Sub on new bronze files.

### Silver → Gold (Kimball Star Schema)

- Create dimension/fact tables (e.g., dim_product, dim_date, fact_trades).
- Materialize as:
  - BigQuery native tables (recommended), or
  - Iceberg tables queried via BigLake.
- Use views in BigQuery for final Kimball schema.

## Phase 5: BI Layer

- Connect Looker Studio to BigQuery `gold_crypto` dataset.
- Build dashboards:
  - Trade volume over time
  - Price movements
  - Top products by volume
- Make dashboards public (share link) for portfolio demo.

## Phase 6: CI/CD and Testing

**GitHub Actions Workflow** (on push/PR and merge):

1. OpenTofu fmt/validate/plan
2. Unit tests (pytest) for function logic (mock Coinbase responses)
3. Integration tests (optional separate test project):
   - Deploy infra
   - Trigger ingestion
   - Assert files in GCS
   - Run Spark job
   - Query BigQuery for expected rows
4. On main merge (with approval): `terraform apply`

**Testing Tips**:
- Mock API calls in unit tests.
- Use local Spark for DV logic testing.
- Keep tests fast and idempotent.

## Validation Milestones (Quick Wins)

1. OpenTofu apply → DataPlex lake + buckets visible.
2. Deploy & trigger ingestion Function → Files land in bronze → BigLake table auto-created.
3. Run Dataproc Spark job → Iceberg tables in silver → Queryable in BigQuery.
4. Build Looker Studio dashboard → Data visualized.
5. CI/CD pipeline runs successfully on a commit.
