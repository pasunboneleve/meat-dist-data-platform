resource "google_bigquery_table" "hub_carcass" {
  project             = var.project_id
  dataset_id          = google_bigquery_dataset.silver_meat_market.dataset_id
  table_id            = "hub_carcass"
  deletion_protection = false

  biglake_configuration {
    connection_id = google_bigquery_connection.biglake.id
    file_format   = "PARQUET"
    storage_uri   = "gs://${google_storage_bucket.silver_bucket.name}/iceberg_warehouse/${local.db_name}/hub_carcass"
    table_format  = "ICEBERG"
  }
}

resource "google_bigquery_table" "hub_plant" {
  project             = var.project_id
  dataset_id          = google_bigquery_dataset.silver_meat_market.dataset_id
  table_id            = "hub_plant"
  deletion_protection = false

  biglake_configuration {
    connection_id = google_bigquery_connection.biglake.id
    file_format   = "PARQUET"
    storage_uri   = "gs://${google_storage_bucket.silver_bucket.name}/iceberg_warehouse/${local.db_name}/hub_plant"
    table_format  = "ICEBERG"
  }
}

resource "google_bigquery_table" "hub_indicator" {
  project             = var.project_id
  dataset_id          = google_bigquery_dataset.silver_meat_market.dataset_id
  table_id            = "hub_indicator"
  deletion_protection = false

  biglake_configuration {
    connection_id = google_bigquery_connection.biglake.id
    file_format   = "PARQUET"
    storage_uri   = "gs://${google_storage_bucket.silver_bucket.name}/iceberg_warehouse/${local.db_name}/hub_indicator"
    table_format  = "ICEBERG"
  }
}

resource "google_bigquery_table" "sat_carcass_detail" {
  project             = var.project_id
  dataset_id          = google_bigquery_dataset.silver_meat_market.dataset_id
  table_id            = "sat_carcass_detail"
  deletion_protection = false

  biglake_configuration {
    connection_id = google_bigquery_connection.biglake.id
    file_format   = "PARQUET"
    storage_uri   = "gs://${google_storage_bucket.silver_bucket.name}/iceberg_warehouse/${local.db_name}/sat_carcass_detail"
    table_format  = "ICEBERG"
  }
}

resource "google_bigquery_table" "link_carcass_plant" {
  project             = var.project_id
  dataset_id          = google_bigquery_dataset.silver_meat_market.dataset_id
  table_id            = "link_carcass_plant"
  deletion_protection = false

  biglake_configuration {
    connection_id = google_bigquery_connection.biglake.id
    file_format   = "PARQUET"
    storage_uri   = "gs://${google_storage_bucket.silver_bucket.name}/iceberg_warehouse/${local.db_name}/link_carcass_plant"
    table_format  = "ICEBERG"
  }
}

resource "google_bigquery_table" "link_carcass_indicator" {
  project             = var.project_id
  dataset_id          = google_bigquery_dataset.silver_meat_market.dataset_id
  table_id            = "link_carcass_indicator"
  deletion_protection = false

  biglake_configuration {
    connection_id = google_bigquery_connection.biglake.id
    file_format   = "PARQUET"
    storage_uri   = "gs://${google_storage_bucket.silver_bucket.name}/iceberg_warehouse/${local.db_name}/link_carcass_indicator"
    table_format  = "ICEBERG"
  }
}

resource "google_bigquery_table" "hub_saleyard" {
  project             = var.project_id
  dataset_id          = google_bigquery_dataset.silver_meat_market.dataset_id
  table_id            = "hub_saleyard"
  deletion_protection = false

  biglake_configuration {
    connection_id = google_bigquery_connection.biglake.id
    file_format   = "PARQUET"
    storage_uri   = "gs://${google_storage_bucket.silver_bucket.name}/iceberg_warehouse/${local.db_name}/hub_saleyard"
    table_format  = "ICEBERG"
  }
}

resource "google_bigquery_table" "sat_saleyard_detail" {
  project             = var.project_id
  dataset_id          = google_bigquery_dataset.silver_meat_market.dataset_id
  table_id            = "sat_saleyard_detail"
  deletion_protection = false

  biglake_configuration {
    connection_id = google_bigquery_connection.biglake.id
    file_format   = "PARQUET"
    storage_uri   = "gs://${google_storage_bucket.silver_bucket.name}/iceberg_warehouse/${local.db_name}/sat_saleyard_detail"
    table_format  = "ICEBERG"
  }
}

resource "google_bigquery_table" "link_carcass_saleyard" {
  project             = var.project_id
  dataset_id          = google_bigquery_dataset.silver_meat_market.dataset_id
  table_id            = "link_carcass_saleyard"
  deletion_protection = false

  biglake_configuration {
    connection_id = google_bigquery_connection.biglake.id
    file_format   = "PARQUET"
    storage_uri   = "gs://${google_storage_bucket.silver_bucket.name}/iceberg_warehouse/${local.db_name}/link_carcass_saleyard"
    table_format  = "ICEBERG"
  }
}
