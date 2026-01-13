
variable "project_id" {
  description = "GCP project ID"
  type        = string
}

variable "project_number" {
  description = "GCP project number"
  type        = string
}

variable "app_engine_region" {
  description = "Region for App Engine and Cloud Scheduler (must support App Engine)"
  type        = string
}

variable "pool_id" {
  description = "Workload Identity Pool ID (e.g., github-pool)"
  type        = string
}

variable "provider_id" {
  description = "Workload Identity Provider ID (e.g., github-provider)"
  type        = string
}

variable "github_owner" {
  description = "GitHub organization or user"
  type        = string
}

variable "github_repo" {
  description = "GitHub repository name"
  type        = string
}

variable "github_token" {
  description = "GitHub Personal Access Token with repo scope for managing repository secrets"
  type        = string
  sensitive   = true
}

variable "tf_state_bucket" {
  description = "GCS bucket for Terraform state"
  type        = string
}

variable "artifact_registry_repository" {
  description = "Artifact Registry repository for synthetic meat"
  type        = string
  default     = "meat-data-images"
}

variable "image_name" {
  description = "synthetic meat image name"
  type        = string
  default     = "synthetic-meat"
}
