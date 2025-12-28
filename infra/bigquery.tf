resource "google_bigquery_dataset" "warehouse" {
  dataset_id = "warehouse"
  project    = var.project_id
  location   = var.region
}
