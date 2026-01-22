# --- BigLake ---

locals {
  db_name = "silver_meat_market_db"
}

resource "google_project_service" "biglake_api" {
  project = var.project_id
  service = "biglake.googleapis.com"

  disable_dependent_services = false
  disable_on_destroy         = false
}

resource "google_biglake_catalog" "meat_iceberg" {
  project    = var.project_id
  location   = var.app_engine_region
  name       = "meat_iceberg_catalog"
  depends_on = [google_project_service.biglake_api]
}

resource "google_biglake_database" "meat_db" {
  name    = "silver_meat_market_db"
  catalog = google_biglake_catalog.meat_iceberg.id
  type    = "HIVE"

  hive_options {
    location_uri = "gs://${google_storage_bucket.silver_bucket.name}/iceberg_warehouse/${local.db_name}"
  }
}

# BigLake connection for querying GCS data from BigQuery
resource "google_bigquery_connection" "biglake" {
  project       = var.project_id
  connection_id = "biglake-connection"
  location      = var.app_engine_region
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
  bucket = google_storage_bucket.bronze_bucket.name
  role   = "roles/storage.objectViewer"
  member = "serviceAccount:${google_bigquery_connection.biglake.cloud_resource[0].service_account_id}"

  depends_on = [time_sleep.wait_for_biglake_sa]
}

resource "google_storage_bucket_iam_member" "biglake_sa_silver_reader" {
  bucket = google_storage_bucket.silver_bucket.name
  role   = "roles/storage.objectAdmin"
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
