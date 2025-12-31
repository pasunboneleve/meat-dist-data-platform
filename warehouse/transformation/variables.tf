variable "project_id" {
  description = "GCP project ID"
  type        = string
}

variable "region" {
  description = "Region for BigQuery resources"
  type        = string
}

variable "silver_bucket_name" {
  description = "Name of the GCS bucket for silver data (Iceberg tables)"
  type        = string
}

variable "biglake_connection_id" {
  description = "The ID of the BigLake connection"
  type        = string
}

variable "silver_dataset_id" {
  description = "The ID of the silver BigQuery dataset"
  type        = string
}
