resource "google_bigquery_table" "hub_carcass" {
  project             = var.project_id
  dataset_id          = google_bigquery_dataset.silver_meat_market.dataset_id
  table_id            = "hub_carcass"
  deletion_protection = false

  schema = <<EOF
[
  {"name": "carcass_hk", "type": "STRING", "mode": "NULLABLE"},
  {"name": "carcass_id", "type": "STRING", "mode": "NULLABLE"},
  {"name": "load_dts", "type": "TIMESTAMP", "mode": "NULLABLE"},
  {"name": "rec_src", "type": "STRING", "mode": "NULLABLE"}
]
EOF

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

  schema = <<EOF
[
  {"name": "plant_hk", "type": "STRING", "mode": "NULLABLE"},
  {"name": "plant_id", "type": "STRING", "mode": "NULLABLE"},
  {"name": "load_dts", "type": "TIMESTAMP", "mode": "NULLABLE"},
  {"name": "rec_src", "type": "STRING", "mode": "NULLABLE"}
]
EOF

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

  schema = <<EOF
[
  {"name": "indicator_hk", "type": "STRING", "mode": "NULLABLE"},
  {"name": "indicator_id", "type": "INTEGER", "mode": "NULLABLE"},
  {"name": "load_dts", "type": "TIMESTAMP", "mode": "NULLABLE"},
  {"name": "rec_src", "type": "STRING", "mode": "NULLABLE"}
]
EOF

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

  schema = <<EOF
[
  {"name": "carcass_hk", "type": "STRING", "mode": "NULLABLE"},
  {"name": "hscw_kg", "type": "BIGNUMERIC", "precision": 10, "scale": 2, "mode": "NULLABLE"},
  {"name": "animal_class", "type": "STRING", "mode": "NULLABLE"},
  {"name": "price_aud_per_kg", "type": "BIGNUMERIC", "precision": 10, "scale": 2, "mode": "NULLABLE"},
  {"name": "marbling_score", "type": "INTEGER", "mode": "NULLABLE"},
  {"name": "quality_score", "type": "INTEGER", "mode": "NULLABLE"},
  {"name": "fat_depth_mm", "type": "INTEGER", "mode": "NULLABLE"},
  {"name": "total_price_aud", "type": "BIGNUMERIC", "precision": 10, "scale": 2, "mode": "NULLABLE"},
  {"name": "slaughter_date", "type": "DATE", "mode": "NULLABLE"},
  {"name": "load_dts", "type": "TIMESTAMP", "mode": "NULLABLE"},
  {"name": "rec_src", "type": "STRING", "mode": "NULLABLE"}
]
EOF

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

  schema = <<EOF
[
  {"name": "carcass_hk", "type": "STRING", "mode": "NULLABLE"},
  {"name": "plant_hk", "type": "STRING", "mode": "NULLABLE"},
  {"name": "load_dts", "type": "TIMESTAMP", "mode": "NULLABLE"},
  {"name": "process_date", "type": "DATE", "mode": "NULLABLE"}
]
EOF

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

  schema = <<EOF
[
  {"name": "carcass_hk", "type": "STRING", "mode": "NULLABLE"},
  {"name": "indicator_hk", "type": "STRING", "mode": "NULLABLE"},
  {"name": "load_dts", "type": "TIMESTAMP", "mode": "NULLABLE"}
]
EOF

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

  schema = <<EOF
[
  {"name": "saleyard_hk", "type": "STRING", "mode": "NULLABLE"},
  {"name": "saleyard_id", "type": "STRING", "mode": "NULLABLE"},
  {"name": "load_dts", "type": "TIMESTAMP", "mode": "NULLABLE"},
  {"name": "rec_src", "type": "STRING", "mode": "NULLABLE"}
]
EOF

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

  schema = <<EOF
[
  {"name": "saleyard_hk", "type": "STRING", "mode": "NULLABLE"},
  {"name": "saleyard_desc", "type": "STRING", "mode": "NULLABLE"},
  {"name": "state_id", "type": "STRING", "mode": "NULLABLE"},
  {"name": "nrmr_desc", "type": "STRING", "mode": "NULLABLE"},
  {"name": "load_dts", "type": "TIMESTAMP", "mode": "NULLABLE"},
  {"name": "rec_src", "type": "STRING", "mode": "NULLABLE"}
]
EOF

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

  schema = <<EOF
[
  {"name": "carcass_hk", "type": "STRING", "mode": "NULLABLE"},
  {"name": "saleyard_hk", "type": "STRING", "mode": "NULLABLE"},
  {"name": "load_dts", "type": "TIMESTAMP", "mode": "NULLABLE"}
]
EOF

  biglake_configuration {
    connection_id = google_bigquery_connection.biglake.id
    file_format   = "PARQUET"
    storage_uri   = "gs://${google_storage_bucket.silver_bucket.name}/iceberg_warehouse/${local.db_name}/link_carcass_saleyard"
    table_format  = "ICEBERG"
  }
}
