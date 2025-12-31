output "carcasses_silver_table_id" {
  description = "The ID of the silver carcasses BigQuery table"
  value       = google_bigquery_table.carcasses_silver.id
}
