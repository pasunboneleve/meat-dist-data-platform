data "google_project" "project" {
  project_id = var.project_id
}

locals {
  # APIs needed for the data warehouse infrastructure
  required_apis = [
    "dataproc.googleapis.com",
    "bigquery.googleapis.com",
    "dataplex.googleapis.com",
    "storage.googleapis.com",
    "run.googleapis.com",
    "cloudscheduler.googleapis.com",
    "bigqueryconnection.googleapis.com",
    "composer.googleapis.com"
  ]
}

# Enable required Google Cloud APIs for the data warehouse
resource "google_project_service" "apis" {
  for_each = toset(local.required_apis)
  project  = var.project_id
  service  = each.value

  disable_dependent_services = false
  disable_on_destroy         = false
}

# --- GCS Buckets ---
resource "google_storage_bucket" "bronze_bucket" {
  project       = var.project_id
  name          = "${var.project_id}-bronze-bucket"
  location      = var.app_engine_region
  force_destroy = true

  depends_on = [google_project_service.apis]
}

resource "google_storage_bucket" "silver_bucket" {
  project       = var.project_id
  name          = "${var.project_id}-silver-bucket"
  location      = var.app_engine_region
  force_destroy = true

  depends_on = [google_project_service.apis]
}

resource "google_storage_bucket" "gold_bucket" {
  project       = var.project_id
  name          = "${var.project_id}-gold-bucket"
  location      = var.app_engine_region
  force_destroy = true

  depends_on = [google_project_service.apis]
}

resource "google_storage_bucket" "deps_bucket" {
  project       = var.project_id
  name          = "${var.project_id}-deps-bucket"
  location      = var.app_engine_region
  force_destroy = true

  depends_on = [google_project_service.apis]
}

# IAM for ingestion to new SE1 bronze bucket
resource "google_storage_bucket_iam_member" "ingestion_sa_bronze_bucket_writer" {
  bucket = google_storage_bucket.bronze_bucket.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.ingestion_sa.email}"
}

# --- Dataplex ---
# Dataplex Lake for centralized management and governance
resource "google_dataplex_lake" "meat_market_lake" {
  project      = var.project_id
  name         = "meat-market-lake"
  location     = var.app_engine_region
  display_name = "Meat Market Data Lake"

  depends_on = [google_project_service.apis]
}

# Raw zone for bronze data
resource "google_dataplex_zone" "raw_zone" {
  project      = var.project_id
  lake         = google_dataplex_lake.meat_market_lake.name
  location     = var.app_engine_region
  name         = "raw"
  display_name = "Raw Zone (Bronze)"
  type         = "RAW"
  discovery_spec {
    enabled = true
  }
  resource_spec {
    location_type = "SINGLE_REGION"
  }
}

# Curated zone for silver data
resource "google_dataplex_zone" "curated_zone" {
  project      = var.project_id
  lake         = google_dataplex_lake.meat_market_lake.name
  location     = var.app_engine_region
  name         = "curated"
  display_name = "Curated Zone (Silver)"
  type         = "CURATED"
  discovery_spec {
    enabled = true
  }
  resource_spec {
    location_type = "SINGLE_REGION"
  }
}

# Link bronze bucket to the raw zone
resource "google_dataplex_asset" "bronze_asset" {
  project       = var.project_id
  lake          = google_dataplex_lake.meat_market_lake.name
  location      = var.app_engine_region
  dataplex_zone = google_dataplex_zone.raw_zone.name
  name          = "bronze-storage"
  display_name  = "Bronze GCS Bucket"
  discovery_spec {
    enabled = true
  }
  resource_spec {
    name = "projects/${google_storage_bucket.bronze_bucket.project}/buckets/${google_storage_bucket.bronze_bucket.name}"
    type = "STORAGE_BUCKET"
  }
}

# Link silver bucket to the curated zone
resource "google_dataplex_asset" "silver_asset" {
  project       = var.project_id
  lake          = google_dataplex_lake.meat_market_lake.name
  location      = var.app_engine_region
  dataplex_zone = google_dataplex_zone.curated_zone.name
  name          = "silver-storage"
  display_name  = "Silver GCS Bucket"
  discovery_spec {
    enabled          = true
    exclude_patterns = ["**/metadata/**"]
  }
  resource_spec {
    name = "projects/${google_storage_bucket.silver_bucket.project}/buckets/${google_storage_bucket.silver_bucket.name}"
    type = "STORAGE_BUCKET"
  }
}

# --- BigQuery ---
# Silver dataset for external tables pointing to the curated zone
resource "google_bigquery_dataset" "silver_meat_market" {
  project    = var.project_id
  dataset_id = "silver_meat_market"
  location   = var.app_engine_region

  depends_on = [google_project_service.apis]
}

# Gold dataset for Kimball models and BI
resource "google_bigquery_dataset" "gold_meat_market" {
  project    = var.project_id
  dataset_id = "gold_meat_market"
  location   = var.app_engine_region

  depends_on = [google_project_service.apis]
}

# --- Dynamically set Python dependencies for DAGs.
data "external" "uv_lock_deps" {
  program     = ["python3", "${path.root}/../scripts/parse_uv_lock.py"]
  working_dir = "${path.root}/../dag"
}

# --- Cloud Composer for Pipeline Orchestration ---
resource "google_composer_environment" "meat_composer" {
  name   = "meat-composer"
  region = var.app_engine_region
  labels = {
    environment = "prod"
  }

  config {
    software_config {
      image_version = "composer-3-airflow-3.1.0-build.6"
      pypi_packages = data.external.uv_lock_deps.result
      env_variables = {
        GCP_PROJECT_ID                 = var.project_id
        DATAPROC_REGION                = var.app_engine_region
        DATAPROC_BATCH_SERVICE_ACCOUNT = google_service_account.dataproc_batch_sa.email
        SYNTHETIC_MEAT_URL             = local.ingestion_service_url
        BRONZE_BUCKET                  = google_storage_bucket.bronze_bucket.name
        SILVER_BUCKET                  = google_storage_bucket.silver_bucket.name
        GOLD_BUCKET                    = google_storage_bucket.gold_bucket.name
        DEPS_BUCKET                    = google_storage_bucket.deps_bucket.name
        CATALOG_NAME                   = google_biglake_catalog.meat_iceberg.name
      }
      airflow_config_overrides = {
        "scheduler-min_file_process_interval" = "30"
        "scheduler-dag_dir_list_interval"     = "30"
      }
    }

    # --- AUTOSCALING CONFIG ---
    workloads_config {
      scheduler {
        cpu        = 0.5
        memory_gb  = 2
        storage_gb = 1
        count      = 1
      }
      web_server {
        cpu        = 0.5
        memory_gb  = 2
        storage_gb = 1
      }
      worker {
        cpu        = 0.5
        memory_gb  = 2
        storage_gb = 1
        min_count  = 1
        max_count  = 6
      }
      triggerer {
        cpu       = 0.5
        memory_gb = 2.0
        count     = 1
      }
      dag_processor {
        cpu        = 0.5 # or 1.0 if budget allows
        memory_gb  = 2
        storage_gb = 1
        count      = 1 # usually 1 is enough for personal use
      }
    }

    # --- DATA RETENTION CONFIG (New) ---
    # Automatically deletes database metadata older than 90 days.
    # This keeps the Airflow database fast and responsive.
    data_retention_config {
      airflow_metadata_retention_config {
        retention_mode = "RETENTION_MODE_ENABLED"
        retention_days = 90
      }
    }

    # --- IDENTITY CONFIG ---
    node_config {
      service_account = google_service_account.dataproc_sa.email
    }
  }

  depends_on = [google_project_service.apis]
}

# send that info to Github Actions
locals {
  composer_bucket = google_composer_environment.meat_composer.config[0].dag_gcs_prefix
}

# --- IAM / Service Accounts ---

# Service account for the ingestion service (temporarily restored for cleanup)
resource "google_service_account" "ingestion_sa" {
  project      = var.project_id
  account_id   = "ingestion-service"
  display_name = "Ingestion Service SA"
  description  = "Service account for the ingestion service"

  depends_on = [google_project_service.apis]
}

# Service account for Dataproc Serverless jobs
resource "google_service_account" "dataproc_sa" {
  project      = var.project_id
  account_id   = "dataproc-serverless"
  display_name = "Dataproc Serverless SA"
  description  = "Service account for Dataproc Serverless Spark jobs"

  depends_on = [google_project_service.apis]
}

# Service account for Cloud Scheduler jobs
resource "google_service_account" "scheduler_sa" {
  project      = var.project_id
  account_id   = "scheduler-invoker"
  display_name = "Cloud Scheduler Invoker SA"
  description  = "Service account for Cloud Scheduler jobs to invoke services"

  depends_on = [google_project_service.apis]
}

# Grant Dataproc SA permissions
resource "google_project_iam_member" "dataproc_sa_roles" {
  for_each = toset([
    "roles/composer.worker",         # Required for Composer worker nodes
    "roles/storage.objectAdmin",     # To read/write from GCS buckets
    "roles/bigquery.dataEditor",     # To read/write BigQuery tables
    "roles/dataplex.metadataReader", # To read metadata from Dataplex
    "roles/dataplex.dataOwner",      # To manage data in Dataplex zones
    "roles/dataproc.editor",         # To submit serverless batches from Composer
    "roles/run.invoker",             # To invoke Cloud Run ingestion service
  ])
  project = var.project_id
  role    = each.value
  member  = "serviceAccount:${google_service_account.dataproc_sa.email}"
}

# Grant Ingestion SA permission to write to the bronze bucket
resource "google_storage_bucket_iam_member" "ingestion_sa_bronze_writer" {
  bucket = google_storage_bucket.bronze_bucket.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.ingestion_sa.email}"
}



# Required for Composer 2+ environment creation: Grant Composer Service Agent V2 Ext role
resource "google_project_iam_member" "composer_service_agent_v2_ext" {
  project = var.project_id
  role    = "roles/composer.ServiceAgentV2Ext"
  member  = "serviceAccount:service-${data.google_project.project.number}@cloudcomposer-accounts.iam.gserviceaccount.com"

  depends_on = [google_project_service.apis]
}

# Wait for IAM propagation before Composer env creation
resource "time_sleep" "wait_composer_permissions" {
  create_duration = "120s"

  depends_on = [google_project_iam_member.composer_service_agent_v2_ext]
}
