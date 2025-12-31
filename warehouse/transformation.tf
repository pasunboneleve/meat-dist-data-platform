data "google_storage_bucket_objects" "carcasses_metadata" {
  bucket = google_storage_bucket.silver.name
  prefix = "carcasses/metadata/"
}

resource "google_bigquery_table" "carcasses_silver" {
  count      = length(data.google_storage_bucket_objects.carcasses_metadata.objects) > 0 ? 1 : 0
  project    = var.project_id
  dataset_id = google_bigquery_dataset.silver_meat_market.dataset_id
  table_id   = "carcasses"

  deletion_protection = false # Good for dev, consider true for prod

  external_data_configuration {
    # The source URI should point to the root folder of the Iceberg table.
    # The Spark job will write data and metadata here.
    # BigQuery uses the BigLake connection to find the latest metadata snapshot.
    source_uris = [
      "gs://${google_storage_bucket.silver.name}/carcasses"
    ]
    source_format = "ICEBERG"
    connection_id = google_bigquery_connection.biglake.id
    autodetect    = false # Schema is managed by the Iceberg metadata
  }
}
