variable "project_id" {
  description = "GCP project ID"
  type        = string
}

variable "region" {
  description = "BigQuery dataset region"
  type        = string
}

variable "image_uri" {
  description = "URI of the container image for the ingestion service"
  type        = string
  default     = "us-docker.pkg.dev/cloudrun/container/hello"
}

variable "app_engine_region" {
  description = "Primary region"
  type        = string
}
