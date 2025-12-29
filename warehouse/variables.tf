variable "project_id" {
  description = "GCP project ID"
  type        = string
}

variable "region" {
  description = "BigQuery dataset region"
  type        = string
}

variable "app_engine_region" {
  description = "Region for App Engine and Cloud Scheduler (must support App Engine)"
  type        = string
}

variable "project_number" {
  description = "GCP project number, used for service account identifiers"
  type        = string
}
