# External tables for Iceberg tables in BigQuery
resource "google_bigquery_table" "hub_carcass" {
  project    = var.project_id
  dataset_id = google_bigquery_dataset.silver_meat_market.dataset_id
  table_id   = "hub_carcass"

  schema = <<EOF
[
    {
      "name": "carcass_id",
      "type": "STRING"
    },
    {
      "name": "load_dts",
      "type": "STRING"
    },
    {
      "name": "rec_src",
      "type": "STRING"
    }
  ]
EOF

  biglake_configuration {
    connection_id = google_bigquery_connection.biglake.id
    storage_uri   = "gs://${google_storage_bucket.silver_bucket.name}/iceberg_warehouse/default/hub_carcass/"
    file_format   = "PARQUET"
    table_format  = "ICEBERG"
  }

  depends_on = [google_bigquery_connection.biglake, google_storage_bucket.silver_bucket, google_storage_bucket_iam_member.biglake_sa_silver_reader]
}

resource "google_bigquery_table" "hub_plant" {
  project    = var.project_id
  dataset_id = google_bigquery_dataset.silver_meat_market.dataset_id
  table_id   = "hub_plant"

  schema = <<EOF
[
    {
      "name": "plant_id",
      "type": "STRING"
    },
    {
      "name": "load_dts",
      "type": "STRING"
    },
    {
      "name": "rec_src",
      "type": "STRING"
    }
  ]
EOF

  biglake_configuration {
    connection_id = google_bigquery_connection.biglake.id
    storage_uri   = "gs://${google_storage_bucket.silver_bucket.name}/iceberg_warehouse/default/hub_plant/"
    file_format   = "PARQUET"
    table_format  = "ICEBERG"
  }

  depends_on = [google_bigquery_connection.biglake, google_storage_bucket.silver_bucket, google_storage_bucket_iam_member.biglake_sa_silver_reader]
}

resource "google_bigquery_table" "hub_indicator" {
  project    = var.project_id
  dataset_id = google_bigquery_dataset.silver_meat_market.dataset_id
  table_id   = "hub_indicator"

  schema = <<EOF
[
    {
      "name": "indicator_id",
      "type": "INT64"
    },
    {
      "name": "load_dts",
      "type": "STRING"
    },
    {
      "name": "rec_src",
      "type": "STRING"
    }
  ]
EOF

  biglake_configuration {
    connection_id = google_bigquery_connection.biglake.id
    storage_uri   = "gs://${google_storage_bucket.silver_bucket.name}/iceberg_warehouse/default/hub_indicator/"
    file_format   = "PARQUET"
    table_format  = "ICEBERG"
  }

  depends_on = [google_bigquery_connection.biglake, google_storage_bucket.silver_bucket, google_storage_bucket_iam_member.biglake_sa_silver_reader]
}

resource "google_bigquery_table" "sat_carcass_detail" {
  project    = var.project_id
  dataset_id = google_bigquery_dataset.silver_meat_market.dataset_id
  table_id   = "sat_carcass_detail"

  schema = <<EOF
[
    {
      "name": "carcass_hk",
      "type": "STRING"
    },
    {
      "name": "hscw_kg",
      "type": "FLOAT64"
    },
    {
      "name": "animal_class",
      "type": "STRING"
    },
    {
      "name": "price_aud_per_kg",
      "type": "FLOAT64"
    },
    {
      "name": "marbling_score",
      "type": "INT64"
    },
    {
      "name": "quality_score",
      "type": "INT64"
    },
    {
      "name": "fat_depth_mm",
      "type": "INT64"
    },
    {
      "name": "total_price_aud",
      "type": "FLOAT64"
    },
    {
      "name": "slaughter_date",
      "type": "DATE"
    },
    {
      "name": "load_dts",
      "type": "STRING"
    },
    {
      "name": "rec_src",
      "type": "STRING"
    }
  ]
EOF

  biglake_configuration {
    connection_id = google_bigquery_connection.biglake.id
    storage_uri   = "gs://${google_storage_bucket.silver_bucket.name}/iceberg_warehouse/default/sat_carcass_detail/"
    file_format   = "PARQUET"
    table_format  = "ICEBERG"
  }

  depends_on = [google_bigquery_connection.biglake, google_storage_bucket.silver_bucket, google_storage_bucket_iam_member.biglake_sa_silver_reader]
}

resource "google_bigquery_table" "link_carcass_plant" {
  project    = var.project_id
  dataset_id = google_bigquery_dataset.silver_meat_market.dataset_id
  table_id   = "link_carcass_plant"

  schema = <<EOF
[
    {
      "name": "carcass_hk",
      "type": "STRING"
    },
    {
      "name": "plant_hk",
      "type": "STRING"
    },
    {
      "name": "load_dts",
      "type": "STRING"
    },
    {
      "name": "process_date",
      "type": "DATE"
    }
  ]
EOF

  biglake_configuration {
    connection_id = google_bigquery_connection.biglake.id
    storage_uri   = "gs://${google_storage_bucket.silver_bucket.name}/iceberg_warehouse/default/link_carcass_plant/"
    file_format   = "PARQUET"
    table_format  = "ICEBERG"
  }

  depends_on = [google_bigquery_connection.biglake, google_storage_bucket.silver_bucket, google_storage_bucket_iam_member.biglake_sa_silver_reader]
}

resource "google_bigquery_table" "link_carcass_indicator" {
  project    = var.project_id
  dataset_id = google_bigquery_dataset.silver_meat_market.dataset_id
  table_id   = "link_carcass_indicator"

  schema = <<EOF
[
    {
      "name": "carcass_hk",
      "type": "STRING"
    },
    {
      "name": "indicator_hk",
      "type": "STRING"
    },
    {
      "name": "load_dts",
      "type": "STRING"
    }
  ]
EOF

  biglake_configuration {
    connection_id = google_bigquery_connection.biglake.id
    storage_uri   = "gs://${google_storage_bucket.silver_bucket.name}/iceberg_warehouse/default/link_carcass_indicator/"
    file_format   = "PARQUET"
    table_format  = "ICEBERG"
  }

  depends_on = [google_bigquery_connection.biglake, google_storage_bucket.silver_bucket]
}
