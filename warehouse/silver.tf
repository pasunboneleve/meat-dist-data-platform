resource "google_biglake_table" "hub_carcass" {
  name     = "hub_carcass"
  database = google_biglake_database.meat_db.id
  type     = "HIVE"
}

resource "google_bigquery_table" "bq_hub_carcass" {
  project             = var.project_id
  dataset_id          = google_bigquery_dataset.silver_meat_market.dataset_id
  table_id            = google_biglake_table.hub_carcass.name
  deletion_protection = false

  external_data_configuration {
    autodetect    = true
    source_format = "BIGLAKE"
    connection_id = google_bigquery_connection.biglake.id
    source_uris   = [google_biglake_table.hub_carcass.id]
  }
}

resource "google_biglake_table" "hub_plant" {
  name     = "hub_plant"
  database = google_biglake_database.meat_db.id
  type     = "HIVE"
}

resource "google_bigquery_table" "bq_hub_plant" {
  project             = var.project_id
  dataset_id          = google_bigquery_dataset.silver_meat_market.dataset_id
  table_id            = google_biglake_table.hub_plant.name
  deletion_protection = false

  external_data_configuration {
    autodetect    = true
    source_format = "BIGLAKE"
    connection_id = google_bigquery_connection.biglake.id
    source_uris   = [google_biglake_table.hub_plant.id]
  }
}

resource "google_biglake_table" "hub_indicator" {
  name     = "hub_indicator"
  database = google_biglake_database.meat_db.id
  type     = "HIVE"
}

resource "google_bigquery_table" "bq_hub_indicator" {
  project             = var.project_id
  dataset_id          = google_bigquery_dataset.silver_meat_market.dataset_id
  table_id            = google_biglake_table.hub_indicator.name
  deletion_protection = false

  external_data_configuration {
    autodetect    = true
    source_format = "BIGLAKE"
    connection_id = google_bigquery_connection.biglake.id
    source_uris   = [google_biglake_table.hub_indicator.id]
  }
}

resource "google_biglake_table" "sat_carcass_detail" {
  name     = "sat_carcass_detail"
  database = google_biglake_database.meat_db.id
  type     = "HIVE"
}

resource "google_bigquery_table" "bq_sat_carcass_detail" {
  project             = var.project_id
  dataset_id          = google_bigquery_dataset.silver_meat_market.dataset_id
  table_id            = google_biglake_table.sat_carcass_detail.name
  deletion_protection = false

  external_data_configuration {
    autodetect    = true
    source_format = "BIGLAKE"
    connection_id = google_bigquery_connection.biglake.id
    source_uris   = [google_biglake_table.sat_carcass_detail.id]
  }
}

resource "google_biglake_table" "link_carcass_plant" {
  name     = "link_carcass_plant"
  database = google_biglake_database.meat_db.id
  type     = "HIVE"
}

resource "google_bigquery_table" "bq_link_carcass_plant" {
  project             = var.project_id
  dataset_id          = google_bigquery_dataset.silver_meat_market.dataset_id
  table_id            = google_biglake_table.link_carcass_plant.name
  deletion_protection = false

  external_data_configuration {
    autodetect    = true
    source_format = "BIGLAKE"
    connection_id = google_bigquery_connection.biglake.id
    source_uris   = [google_biglake_table.link_carcass_plant.id]
  }
}

resource "google_biglake_table" "link_carcass_indicator" {
  name     = "link_carcass_indicator"
  database = google_biglake_database.meat_db.id
  type     = "HIVE"
}

resource "google_bigquery_table" "bq_link_carcass_indicator" {
  project             = var.project_id
  dataset_id          = google_bigquery_dataset.silver_meat_market.dataset_id
  table_id            = google_biglake_table.link_carcass_indicator.name
  deletion_protection = false

  external_data_configuration {
    autodetect    = true
    source_format = "BIGLAKE"
    connection_id = google_bigquery_connection.biglake.id
    source_uris   = [google_biglake_table.link_carcass_indicator.id]
  }
}

resource "google_biglake_table" "hub_saleyard" {
  name     = "hub_saleyard"
  database = google_biglake_database.meat_db.id
  type     = "HIVE"
}

resource "google_bigquery_table" "bq_hub_saleyard" {
  project             = var.project_id
  dataset_id          = google_bigquery_dataset.silver_meat_market.dataset_id
  table_id            = google_biglake_table.hub_saleyard.name
  deletion_protection = false

  external_data_configuration {
    autodetect    = true
    source_format = "BIGLAKE"
    connection_id = google_bigquery_connection.biglake.id
    source_uris   = [google_biglake_table.hub_saleyard.id]
  }
}

resource "google_biglake_table" "sat_saleyard_detail" {
  name     = "sat_saleyard_detail"
  database = google_biglake_database.meat_db.id
  type     = "HIVE"
}

resource "google_bigquery_table" "bq_sat_saleyard_detail" {
  project             = var.project_id
  dataset_id          = google_bigquery_dataset.silver_meat_market.dataset_id
  table_id            = google_biglake_table.sat_saleyard_detail.name
  deletion_protection = false

  external_data_configuration {
    autodetect    = true
    source_format = "BIGLAKE"
    connection_id = google_bigquery_connection.biglake.id
    source_uris   = [google_biglake_table.sat_saleyard_detail.id]
  }
}

resource "google_biglake_table" "link_carcass_saleyard" {
  name     = "link_carcass_saleyard"
  database = google_biglake_database.meat_db.id
  type     = "HIVE"
}

resource "google_bigquery_table" "bq_link_carcass_saleyard" {
  project             = var.project_id
  dataset_id          = google_bigquery_dataset.silver_meat_market.dataset_id
  table_id            = google_biglake_table.link_carcass_saleyard.name
  deletion_protection = false

  external_data_configuration {
    autodetect    = true
    source_format = "BIGLAKE"
    connection_id = google_bigquery_connection.biglake.id
    source_uris   = [google_biglake_table.link_carcass_saleyard.id]
  }
}
