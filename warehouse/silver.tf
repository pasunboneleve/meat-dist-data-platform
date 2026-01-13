# External tables for Iceberg tables in BigQuery
resource "google_bigquery_table" "hub_carcass" {
  project    = var.project_id
  dataset_id = google_bigquery_dataset.silver_meat_market.dataset_id
  table_id   = "hub_carcass"

  biglake_configuration {
    connection_id = google_bigquery_connection.biglake.id
    storage_uri   = "gs://${google_storage_bucket.silver_bucket.name}/iceberg_warehouse/default/hub_carcass/"
    file_format   = "PARQUET"
    source_format = "ICEBERG"
  }

  depends_on = [google_bigquery_connection.biglake, google_storage_bucket.silver_bucket]
}

resource "google_bigquery_table" "hub_plant" {
  project    = var.project_id
  dataset_id = google_bigquery_dataset.silver_meat_market.dataset_id
  table_id   = "hub_plant"

  biglake_configuration {
    autodetect    = true
    connection_id = google_bigquery_connection.biglake.id
    source_format = "ICEBERG"
    source_uris   = ["gs://${google_storage_bucket.silver_bucket.name}/iceberg_warehouse/default/hub_plant/metadata/"]
  }

  depends_on = [google_bigquery_connection.biglake, google_storage_bucket.silver_bucket]
}

resource "google_bigquery_table" "hub_indicator" {
  project    = var.project_id
  dataset_id = google_bigquery_dataset.silver_meat_market.dataset_id
  table_id   = "hub_indicator"

  biglake_configuration {
    autodetect    = true
    connection_id = google_bigquery_connection.biglake.id
    source_format = "ICEBERG"
    source_uris   = ["gs://${google_storage_bucket.silver_bucket.name}/iceberg_warehouse/default/hub_indicator/metadata/"]
  }

  depends_on = [google_bigquery_connection.biglake, google_storage_bucket.silver_bucket]
}

resource "google_bigquery_table" "sat_carcass_detail" {
  project    = var.project_id
  dataset_id = google_bigquery_dataset.silver_meat_market.dataset_id
  table_id   = "sat_carcass_detail"

  biglake_configuration {
    autodetect    = true
    connection_id = google_bigquery_connection.biglake.id
    source_format = "ICEBERG"
    source_uris   = ["gs://${google_storage_bucket.silver_bucket.name}/iceberg_warehouse/default/sat_carcass_detail/metadata/"]
  }

  depends_on = [google_bigquery_connection.biglake, google_storage_bucket.silver_bucket]
}

resource "google_bigquery_table" "link_carcass_plant" {
  project    = var.project_id
  dataset_id = google_bigquery_dataset.silver_meat_market.dataset_id
  table_id   = "link_carcass_plant"

  biglake_configuration {
    autodetect    = true
    connection_id = google_bigquery_connection.biglake.id
    source_format = "ICEBERG"
    source_uris   = ["gs://${google_storage_bucket.silver_bucket.name}/iceberg_warehouse/default/link_carcass_plant/metadata/"]
  }

  depends_on = [google_bigquery_connection.biglake, google_storage_bucket.silver_bucket]
}

resource "google_bigquery_table" "link_carcass_indicator" {
  project    = var.project_id
  dataset_id = google_bigquery_dataset.silver_meat_market.dataset_id
  table_id   = "link_carcass_indicator"

  biglake_configuration {
    autodetect    = true
    connection_id = google_bigquery_connection.biglake.id
    source_format = "ICEBERG"
    source_uris   = ["gs://${google_storage_bucket.silver_bucket.name}/iceberg_warehouse/default/link_carcass_indicator/metadata/"]
  }

  depends_on = [google_bigquery_connection.biglake, google_storage_bucket.silver_bucket]
}
