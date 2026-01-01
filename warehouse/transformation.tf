resource "google_bigquery_table" "carcasses_silver" {
  project             = var.project_id
  dataset_id          = google_bigquery_dataset.silver_meat_market.dataset_id
  table_id            = "carcasses"
  deletion_protection = false

  external_data_configuration {
    source_format = "ICEBERG"
    source_uris   = ["gs://${google_storage_bucket.silver.name}/carcasses"]
    autodetect    = false
  }
}
