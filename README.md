# Modern Open Lakehouse Portfolio Project on Google Cloud

[![CI/CD Status](https://github.com/pasunboneleve/meat-dist-data-platform/actions/workflows/deploy.yml/badge.svg)](https://github.com/pasunboneleve/meat-dist-data-platform/actions/workflows/deploy.yml) [![Ingestion Tests](https://github.com/pasunboneleve/meat-dist-data-platform/actions/workflows/ingestion.yml/badge.svg)](https://github.com/pasunboneleve/meat-dist-data-platform/actions/workflows/ingestion.yml) [![DAG Deploy](https://github.com/pasunboneleve/meat-dist-data-platform/actions/workflows/dag.yml/badge.svg)](https://github.com/pasunboneleve/meat-dist-data-platform/actions/workflows/dag.yml)

**Goal**: Build a cost-effective (~$10–30/month), serverless-first Lakehouse for a meat distribution platform, demonstrating modern data engineering practices (Iceberg, DataPlex, Data Vault 2.0 + Kimball, Terraform IaC, CI/CD).

**Key Technologies**:
- **Data Source**: A synthetic data generator that simulates a stream of meat processing data.
- **Ingestion**: Cloud Run service (Python container) triggered by Cloud Scheduler.
- **Bronze Layer**: Raw JSON/Parquet files in GCS.
- **Silver Layer**: Data Vault 2.0 modeled Iceberg tables in GCS.
- **Gold Layer**: Kimball star schema views or materialized tables queried via BigQuery (over Iceberg/BigLake).
- **Transformations**: Dataproc Serverless Spark (PySpark) batches for Iceberg support.
- **Catalog & Governance**: DataPlex Universal Catalog (auto-discovery, lineage).
- **BI**: Looker Studio (free) public dashboards.
- **IaC**: OpenTofu for everything, configured using HCL.
- **CI/CD & Testing**: GitHub Actions (lint, plan, tests, apply on merge).

## Project Structure

```
repo-root/
├── .github/workflows/        # GitHub Actions CI/CD pipelines
│   ├── deploy.yml
│   └── ingestion.yml
├── infra/                    # Core infrastructure (WIF, deploy SA, permissions)
│   ├── main.tf
│   └── ...
├── ingestion/
│   └── synthetic-meat/       # Source for the Cloud Run ingestion service
│       ├── src/
│       ├── tests/
│       ├── Dockerfile
│       └── pyproject.toml
├── warehouse/                # Data platform infrastructure (GCS, Dataplex, etc.)
│   ├── main.tf
│   └── ...
└── README.md
```

## Phase 1: Project Setup (1–2 hours)

1. Create a new GCP project, enable billing.
2. Enable required APIs:
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
  - Lake: `meat-market-lake`
  - Zones: `raw` (bronze), `curated` (silver)
  - Assets linking buckets to zones
- BigQuery dataset: `gold_meat_market`
- Service accounts & IAM:
  - One for Dataproc (BigQuery, Storage, DataPlex roles)
- BigLake connection (if needed for Iceberg catalog)

Use community modules where possible (e.g., GoogleCloudPlatform/cloud-foundation-fabric).

Validate locally: `tofu init → fmt → validate → plan → apply`.

## Deployment Process

This repository uses a two-part deployment strategy:

1.  **Core Infrastructure (`infra/`)**: This configuration sets up the foundational components for CI/CD, including the Workload Identity Federation, the deployment service account, and its project-level IAM permissions. Because it grants powerful permissions, **it is designed to be applied manually from a local machine** after careful review. Any changes to IAM roles in `infra/main.tf` must be applied locally before they will take effect in the CI/CD pipeline.

    ```bash
    # From your local machine, inside the infra/ directory
    tofu apply -var-file="prod.tfvars"
    ```

2.  **Warehouse Infrastructure (`warehouse/`)**: This configuration defines the application-specific infrastructure, such as GCS buckets, and Dataplex assets. It is deployed automatically by the GitHub Actions workflow (`.github/workflows/deploy.yml`) whenever changes are pushed to the `warehouse/` directory.

## Phase 3: Data Generation & Ingestion to Bronze

- **Data Source**: A synthetic data generator script (Python) that simulates a stream of meat processing data.
- **Methodology**:
  - Use aggregated public data (e.g., from [MLA](https://www.mla.com.au/prices-markets/)) as a baseline for realistic distributions of weights (e.g., 250-400kg HSCW), grades, and prices.
  - Use a library like `polars` to generate thousands of "fake" individual animal/carcass records.
  - Sample attributes like weight from normal distributions based on grade and animal class.
  - Assign pseudo-random identifiers (e.g., RFID-style tags) for traceability.
  - Calculate prices based on grid formulas, applying premiums/discounts for factors like marbling, fat depth, and yield.
  - Include additional fields for rich analytics, such as slaughter date, processing plant ID, breed, and quality scores.
- **Container**: The data generation logic is packaged as a Docker container and deployed as a serverless Cloud Run service.
- **Execution**: A Cloud Scheduler job triggers the Cloud Run service via an HTTP request on a daily schedule.
  - The service generates a new batch of data upon each invocation.
  - It converts the generated data to Parquet format.
  - It writes the partitioned data to the bronze GCS bucket, e.g., `gs://bronze/carcasses/plant_id=P01/year=2025/month=12/day=27/batch_12345.parquet`
- **Discovery**: DataPlex automatically discovers the new Parquet files as they land, making them available for querying via BigLake.

## Phase 4: Transformations

### Bronze → Silver (Data Vault 2.0 with Iceberg)

Use Dataproc Serverless PySpark batch:

- Catalog: BigLakeCatalog (integrated with DataPlex).
- Read bronze Parquet.
- Build DV2 entities:
  - Hub_Carcass (business key: carcass_id/rfid_tag)
  - Hub_Processor (business key: plant_id)
  - Sat_Carcass_Details (quality scores, weights, grades)
  - Link_Carcass_Processing (linking carcasses to processing events)
- Write as Iceberg tables in silver bucket, partitioned appropriately.
- Trigger initially manual (`gcloud dataproc batches submit`), later via Scheduler or Pub/Sub on new bronze files.

### Silver → Gold (Kimball Star Schema)

- Create dimension/fact tables (e.g., dim_product, dim_date, fact_trades).
- Materialize as:
  - BigQuery native tables (recommended), or
  - Iceberg tables queried via BigLake.
- Use views in BigQuery for final Kimball schema.

## Phase 5: BI Layer

- Connect Looker Studio to BigQuery `gold_meat_market` dataset.
- Build dashboards:
  - Carcass weight distribution by grade
  - Average price per kg over time
  - Yield analysis by processing plant
- Make dashboards public (share link) for portfolio demo.

## Phase 6: CI/CD and Testing

**GitHub Actions Workflow** (on push/PR and merge):

1. OpenTofu fmt/validate/plan
2. Unit tests (pytest) for the data generator and transformation logic.
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
2. Run data generator → Files land in bronze → BigLake table auto-created.
3. Run Dataproc Spark job → Iceberg tables in silver → Queryable in BigQuery.
4. Build Looker Studio dashboard → Data visualized.
5. CI/CD pipeline runs successfully on a commit.
