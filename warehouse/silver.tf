resource "google_biglake_table" "hub_carcass" {
  name     = "hub_carcass"
  database = google_biglake_database.meat_db.id
  type     = "HIVE"
}

resource "google_biglake_table" "hub_plant" {
  name     = "hub_plant"
  database = google_biglake_database.meat_db.id
  type     = "HIVE"
}

resource "google_biglake_table" "hub_indicator" {
  name     = "hub_indicator"
  database = google_biglake_database.meat_db.id
  type     = "HIVE"
}

resource "google_biglake_table" "sat_carcass_detail" {
  name     = "sat_carcass_detail"
  database = google_biglake_database.meat_db.id
  type     = "HIVE"
}

resource "google_biglake_table" "link_carcass_plant" {
  name     = "link_carcass_plant"
  database = google_biglake_database.meat_db.id
  type     = "HIVE"
}

resource "google_biglake_table" "link_carcass_indicator" {
  name     = "link_carcass_indicator"
  database = google_biglake_database.meat_db.id
  type     = "HIVE"
}

resource "google_biglake_table" "hub_saleyard" {
  name     = "hub_saleyard"
  database = google_biglake_database.meat_db.id
  type     = "HIVE"
}

resource "google_biglake_table" "sat_saleyard_detail" {
  name     = "sat_saleyard_detail"
  database = google_biglake_database.meat_db.id
  type     = "HIVE"
}

resource "google_biglake_table" "link_carcass_saleyard" {
  name     = "link_carcass_saleyard"
  database = google_biglake_database.meat_db.id
  type     = "HIVE"
}
