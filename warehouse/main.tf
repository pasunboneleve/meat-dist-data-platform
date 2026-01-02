# Re-trigger CI/CD after local infra apply.
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
# Bronze bucket for raw data ingestion
resource "google_storage_bucket" "bronze" {
  project       = var.project_id
  name          = "${var.project_id}-bronze"
  location      = var.region
  force_destroy = true # Good for dev, consider false for prod

  depends_on = [google_project_service.apis, time_sleep.wait_composer_permissions]
}

# Silver bucket for curated, structured data (e.g., Iceberg tables)
resource "google_storage_bucket" "silver" {
  project       = var.project_id
  name          = "${var.project_id}-silver"
  location      = var.region
  force_destroy = true

  depends_on = [google_project_service.apis]
}

# Dependencies bucket for Spark jobs, temp files, etc.
resource "google_storage_bucket" "deps" {
  project       = var.project_id
  name          = "${var.project_id}-deps"
  location      = var.region
  force_destroy = true

  depends_on = [google_project_service.apis]
}


# --- BigLake Connection ---
# BigLake connection for querying GCS data from BigQuery
resource "google_bigquery_connection" "biglake" {
  project       = var.project_id
  connection_id = "biglake-connection"
  location      = var.region
  friendly_name = "BigLake GCS Connection"
  description   = "Connection for BigQuery to read GCS data via BigLake"
  cloud_resource {} # This empty block specifies it's for GCS

  depends_on = [google_project_service.apis]
}

# Wait 30s for the BigLake connection's service account to be created and propagate.
resource "time_sleep" "wait_for_biglake_sa" {
  create_duration = "30s"
  depends_on      = [google_bigquery_connection.biglake]
}

# Grant the BigLake connection's service account access to the GCS buckets.
resource "google_storage_bucket_iam_member" "biglake_sa_bronze_reader" {
  bucket = google_storage_bucket.bronze.name
  role   = "roles/storage.objectViewer"
  member = "serviceAccount:${google_bigquery_connection.biglake.cloud_resource[0].service_account_id}"

  depends_on = [time_sleep.wait_for_biglake_sa]
}

resource "google_storage_bucket_iam_member" "biglake_sa_silver_reader" {
  bucket = google_storage_bucket.silver.name
  role   = "roles/storage.objectViewer"
  member = "serviceAccount:${google_bigquery_connection.biglake.cloud_resource[0].service_account_id}"

  depends_on = [time_sleep.wait_for_biglake_sa]
}

# Grant the Dataplex service account permission to use the BigLake connection
resource "google_bigquery_connection_iam_member" "dataplex_sa_connection_user" {
  project       = google_bigquery_connection.biglake.project
  location      = google_bigquery_connection.biglake.location
  connection_id = google_bigquery_connection.biglake.connection_id
  role          = "roles/bigquery.connectionUser"
  member        = "serviceAccount:service-${data.google_project.project.number}@gcp-sa-dataplex.iam.gserviceaccount.com"
}

# Grant the Dataplex service account BigQuery User role for table creation
resource "google_project_iam_member" "dataplex_sa_bigquery_user" {
  project = var.project_id
  role    = "roles/bigquery.user"
  member  = "serviceAccount:service-${data.google_project.project.number}@gcp-sa-dataplex.iam.gserviceaccount.com"
}


# --- Dataplex ---
# Dataplex Lake for centralized management and governance
resource "google_dataplex_lake" "meat_market_lake" {
  project      = var.project_id
  name         = "meat-market-lake"
  location     = var.region
  display_name = "Meat Market Data Lake"

  depends_on = [google_project_service.apis]
}

# Raw zone for bronze data
resource "google_dataplex_zone" "raw_zone" {
  project                = var.project_id
  lake                   = google_dataplex_lake.meat_market_lake.name
  location               = var.region
  name                   = "raw"
  display_name           = "Raw Zone (Bronze)"
  type                   = "RAW"
  discovery_spec {
    enabled = true
  }
  resource_spec {
    location_type = "SINGLE_REGION"
  }
}

# Curated zone for silver data
resource "google_dataplex_zone" "curated_zone" {
  project                = var.project_id
  lake                   = google_dataplex_lake.meat_market_lake.name
  location               = var.region
  name                   = "curated"
  display_name           = "Curated Zone (Silver)"
  type                   = "CURATED"
  discovery_spec {
    enabled = true
  }
  resource_spec {
    location_type = "SINGLE_REGION"
  }
}

# Link bronze bucket to the raw zone
resource "google_dataplex_asset" "bronze_asset" {
  project          = var.project_id
  lake             = google_dataplex_lake.meat_market_lake.name
  location         = var.region
  dataplex_zone    = google_dataplex_zone.raw_zone.name
  name             = "bronze-storage"
  display_name     = "Bronze GCS Bucket"
  discovery_spec {
    enabled = true
  }
  resource_spec {
    name = "projects/${google_storage_bucket.bronze.project}/buckets/${google_storage_bucket.bronze.name}"
    type = "STORAGE_BUCKET"
  }
}

# Link silver bucket to the curated zone
resource "google_dataplex_asset" "silver_asset" {
  project          = var.project_id
  lake             = google_dataplex_lake.meat_market_lake.name
  location         = var.region
  dataplex_zone    = google_dataplex_zone.curated_zone.name
  name             = "silver-storage"
  display_name     = "Silver GCS Bucket"
  discovery_spec {
    enabled = true
  }
  resource_spec {
    name = "projects/${google_storage_bucket.silver.project}/buckets/${google_storage_bucket.silver.name}"
    type = "STORAGE_BUCKET"
  }
}

# --- BigQuery ---
# Silver dataset for external tables pointing to the curated zone
resource "google_bigquery_dataset" "silver_meat_market" {
  project    = var.project_id
  dataset_id = "silver_meat_market"
  location   = var.region

  depends_on = [google_project_service.apis]
}

# Gold dataset for Kimball models and BI
resource "google_bigquery_dataset" "gold_meat_market" {
  project    = var.project_id
  dataset_id = "gold_meat_market"
  location   = var.region

  depends_on = [google_project_service.apis]
}

# --- Cloud Composer for Pipeline Orchestration ---
resource "google_composer_environment" "meat_composer" {
  name   = "meat-composer"
  region = var.region
  labels = {
    environment = "prod"
  }

  config {
    software_config {
      image_version = "composer-2.16.1-airflow-2.9.3"
    }

    environment_size = "ENVIRONMENT_SIZE_SMALL"

    node_config {
      service_account = google_service_account.dataproc_sa.email
    }
  }

  depends_on = [google_project_service.apis]
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
    "roles/storage.objectAdmin",     # To read/write from GCS buckets
    "roles/bigquery.dataEditor",     # To read/write BigQuery tables
    "roles/dataplex.metadataReader", # To read metadata from Dataplex
    "roles/dataplex.dataOwner",      # To manage data in Dataplex zones
    "roles/dataproc.editor"          # To submit serverless batches from Composer
  ])
  project = var.project_id
  role    = each.value
  member  = "serviceAccount:${google_service_account.dataproc_sa.email}"
}

# Grant Ingestion SA permission to write to the bronze bucket
resource "google_storage_bucket_iam_member" "ingestion_sa_bronze_writer" {
  bucket = google_storage_bucket.bronze.name
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
  create_duration = "30s"

  depends_on = [google_project_iam_member.composer_service_agent_v2_ext]
}
