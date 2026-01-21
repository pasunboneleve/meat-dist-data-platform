# External tables for Iceberg tables in BigQuery
# resource "google_bigquery_table" "hub_carcass" {
#   project             = var.project_id
#   dataset_id          = google_bigquery_dataset.silver_meat_market.dataset_id
#   table_id            = "hub_carcass"
#   deletion_protection = false

#   external_data_configuration {
#     autodetect            = true
#     ignore_unknown_values = true
#     max_bad_records       = 0
#     connection_id         = google_bigquery_connection.biglake.id
#     source_uris           = ["gs://${google_storage_bucket.silver_bucket.name}/iceberg_warehouse/default/hub_carcass/metadata/v6.metadata.json"]
#     source_format         = "ICEBERG"
#   }

#   depends_on = [google_bigquery_connection.biglake, google_storage_bucket.silver_bucket, google_storage_bucket_iam_member.biglake_sa_silver_reader]
# }

# resource "google_bigquery_table" "hub_plant" {
#   project             = var.project_id
#   dataset_id          = google_bigquery_dataset.silver_meat_market.dataset_id
#   table_id            = "hub_plant"
#   deletion_protection = false

#   external_data_configuration {
#     autodetect            = true
#     ignore_unknown_values = true
#     max_bad_records       = 0
#     connection_id         = google_bigquery_connection.biglake.id
#     source_uris           = ["gs://${google_storage_bucket.silver_bucket.name}/iceberg_warehouse/default/hub_plant/metadata/v6.metadata.json"]
#     source_format         = "ICEBERG"
#   }

#   depends_on = [google_bigquery_connection.biglake, google_storage_bucket.silver_bucket, google_storage_bucket_iam_member.biglake_sa_silver_reader]
# }

# resource "google_bigquery_table" "hub_indicator" {
#   project             = var.project_id
#   dataset_id          = google_bigquery_dataset.silver_meat_market.dataset_id
#   table_id            = "hub_indicator"
#   deletion_protection = false

#   external_data_configuration {
#     autodetect            = true
#     ignore_unknown_values = true
#     max_bad_records       = 0
#     connection_id         = google_bigquery_connection.biglake.id
#     source_uris           = ["gs://${google_storage_bucket.silver_bucket.name}/iceberg_warehouse/default/hub_indicator/metadata/v6.metadata.json"]
#     source_format         = "ICEBERG"
#   }

#   depends_on = [google_bigquery_connection.biglake, google_storage_bucket.silver_bucket, google_storage_bucket_iam_member.biglake_sa_silver_reader]
# }

# resource "google_bigquery_table" "sat_carcass_detail" {
#   project             = var.project_id
#   dataset_id          = google_bigquery_dataset.silver_meat_market.dataset_id
#   table_id            = "sat_carcass_detail"
#   deletion_protection = false

#   external_data_configuration {
#     autodetect            = true
#     ignore_unknown_values = true
#     max_bad_records       = 0
#     connection_id         = google_bigquery_connection.biglake.id
#     source_uris           = ["gs://${google_storage_bucket.silver_bucket.name}/iceberg_warehouse/default/sat_carcass_detail/metadata/v6.metadata.json"]
#     source_format         = "ICEBERG"
#   }

#   depends_on = [google_bigquery_connection.biglake, google_storage_bucket.silver_bucket, google_storage_bucket_iam_member.biglake_sa_silver_reader]
# }

# resource "google_bigquery_table" "link_carcass_plant" {
#   project             = var.project_id
#   dataset_id          = google_bigquery_dataset.silver_meat_market.dataset_id
#   table_id            = "link_carcass_plant"
#   deletion_protection = false

#   external_data_configuration {
#     autodetect            = true
#     ignore_unknown_values = true
#     max_bad_records       = 0
#     connection_id         = google_bigquery_connection.biglake.id
#     source_uris           = ["gs://${google_storage_bucket.silver_bucket.name}/iceberg_warehouse/default/link_carcass_plant/metadata/v6.metadata.json"]
#     source_format         = "ICEBERG"
#   }

#   depends_on = [google_bigquery_connection.biglake, google_storage_bucket.silver_bucket, google_storage_bucket_iam_member.biglake_sa_silver_reader]
# }

# resource "google_bigquery_table" "link_carcass_indicator" {
#   project             = var.project_id
#   dataset_id          = google_bigquery_dataset.silver_meat_market.dataset_id
#   table_id            = "link_carcass_indicator"
#   deletion_protection = false

#   external_data_configuration {
#     autodetect            = true
#     ignore_unknown_values = true
#     max_bad_records       = 0
#     connection_id         = google_bigquery_connection.biglake.id
#     source_uris           = ["gs://${google_storage_bucket.silver_bucket.name}/iceberg_warehouse/default/link_carcass_indicator/metadata/v2.metadata.json"]
#     source_format         = "ICEBERG"
#   }

#   depends_on = [google_bigquery_connection.biglake, google_storage_bucket.silver_bucket]
# }

# resource "google_bigquery_table" "hub_saleyard" {
#   project             = var.project_id
#   dataset_id          = google_bigquery_dataset.silver_meat_market.dataset_id
#   table_id            = "hub_saleyard"
#   deletion_protection = false

#   external_data_configuration {
#     autodetect            = true
#     ignore_unknown_values = true
#     max_bad_records       = 0
#     connection_id         = google_bigquery_connection.biglake.id
#     source_uris           = ["gs://${google_storage_bucket.silver_bucket.name}/iceberg_warehouse/default/hub_saleyard/metadata/v2.metadata.json"]
#     source_format         = "ICEBERG"
#   }

#   depends_on = [google_bigquery_connection.biglake, google_storage_bucket.silver_bucket, google_storage_bucket_iam_member.biglake_sa_silver_reader]
# }

# resource "google_bigquery_table" "sat_saleyard_detail" {
#   project             = var.project_id
#   dataset_id          = google_bigquery_dataset.silver_meat_market.dataset_id
#   table_id            = "sat_saleyard_detail"
#   deletion_protection = false

#   external_data_configuration {
#     autodetect            = true
#     ignore_unknown_values = true
#     max_bad_records       = 0
#     connection_id         = google_bigquery_connection.biglake.id
#     source_uris           = ["gs://${google_storage_bucket.silver_bucket.name}/iceberg_warehouse/default/sat_saleyard_detail/metadata/v2.metadata.json"]
#     source_format         = "ICEBERG"
#   }

#   depends_on = [google_bigquery_connection.biglake, google_storage_bucket.silver_bucket, google_storage_bucket_iam_member.biglake_sa_silver_reader]
# }

# resource "google_bigquery_table" "link_carcass_saleyard" {
#   project             = var.project_id
#   dataset_id          = google_bigquery_dataset.silver_meat_market.dataset_id
#   table_id            = "link_carcass_saleyard"
#   deletion_protection = false

#   external_data_configuration {
#     autodetect            = true
#     ignore_unknown_values = true
#     max_bad_records       = 0
#     connection_id         = google_bigquery_connection.biglake.id
#     source_uris           = ["gs://${google_storage_bucket.silver_bucket.name}/iceberg_warehouse/default/link_carcass_saleyard/metadata/v2.metadata.json"]
#     source_format         = "ICEBERG"
#   }

#   depends_on = [google_bigquery_connection.biglake, google_storage_bucket.silver_bucket, google_storage_bucket_iam_member.biglake_sa_silver_reader]
# }
