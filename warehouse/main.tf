locals {
  # APIs needed for the data warehouse infrastructure
  required_apis = [
    "cloudfunctions.googleapis.com",
    "cloudscheduler.googleapis.com",
    "cloudbuild.googleapis.com",
    "dataproc.googleapis.com",
    "bigquery.googleapis.com",
    "dataplex.googleapis.com",
    "storage.googleapis.com"
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

resource "google_bigquery_dataset" "warehouse" {
  project    = var.project_id
  dataset_id = "warehouse"
  location   = var.region

  depends_on = [google_project_service.apis]
}
