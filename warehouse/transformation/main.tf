resource "google_bigquery_table" "carcasses_silver" {
  project    = var.project_id
  dataset_id = var.silver_dataset_id
  table_id   = "carcasses"

  type                = "EXTERNAL_TABLE"
  deletion_protection = false # Good for dev, consider true for prod

  external_data_configuration {
    # The source URI should point to the root folder of the Iceberg table.
    # The Spark job will write data and metadata here.
    # BigQuery uses the BigLake connection to find the latest metadata snapshot.
    source_uris = [
      "gs://${var.silver_bucket_name}/carcasses"
    ]
    source_format = "ICEBERG"
    connection_id = var.biglake_connection_id
    autodetect    = false # Schema is managed by the Iceberg metadata
  }
}
