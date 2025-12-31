resource "google_bigquery_table" "carcasses_silver" {
  project             = var.project_id
  dataset_id          = google_bigquery_dataset.silver_meat_market.dataset_id
  table_id            = "carcasses"
  deletion_protection = false

  table_format = "ICEBERG"  # This makes it a managed BigLake Iceberg table
  file_format  = "PARQUET"  # Or ORC/AVRO if preferred

  options {
    storage_uri = "gs://${google_storage_bucket.silver.name}/carcasses/"  # Root folder
  }

  # Connection for BigLake access (required for managed tables)
  with_connection {
    connection_id = "${var.project_id}.${google_bigquery_connection.biglake.name}"  # Full qualified ID
  }

  # Optional: Define schema upfront if you want (otherwise Spark job defines it on first write)
  # schema = file("path/to/schema.json")
}
