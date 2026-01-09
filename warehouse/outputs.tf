output "composer_bucket" {
  value = local.composer_bucket
}

output "ingestion_service_name" {
  value = local.ingestion_service_name
}

output "ingestion_service_url" {
  value = local.ingestion_service_url
}

output "bronze_bucket" {
  value = google_storage_bucket.bronze_bucket.name
}

output "silver_bucket" {
  value = google_storage_bucket.silver_bucket.name
}

output "gold_bucket" {
  value = google_storage_bucket.gold_bucket.name
}

output "biglake_catalog" {
  value = google_biglake_catalog.meat_iceberg.name
}

output "dataproc_batch_sa_email" {
  value       = google_service_account.dataproc_batch_sa.email
  description = "Service account email to use in Dataproc batches"
}
