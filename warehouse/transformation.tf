resource "google_bigquery_table" "carcasses_silver" {
  project             = var.project_id
  dataset_id          = google_bigquery_dataset.silver_meat_market.dataset_id
  table_id            = "carcasses"
  deletion_protection = false

  external_data_configuration {
    table_format = "ICEBERG"  # This makes it a managed BigLake Iceberg table
    storage_uris = ["gs://${google_storage_bucket.silver.name}/carcasses/"]
  }
}
