resource "google_bigquery_dataset" "warehouse" {
  project    = var.project_id
  dataset_id = "warehouse"
  location   = var.location
}
