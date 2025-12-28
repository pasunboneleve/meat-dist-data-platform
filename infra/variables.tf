variable "gcp_owner" {
  description = "GCP owner's email"
  type        = string
}

variable "repository_id" {
  description = "GCP repository ID"
  type        = string
}

variable "project_id" {
  description = "GCP project ID"
  type        = string
}

variable "project_number" {
  description = "GCP project number"
  type        = string
}

variable "region" {
  description = "Default region for APIs that require one"
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

variable "cloud_run_url" {
  description = "Cloud Run service URL for CNAME record. Find this in Google Cloud Console: Cloud Run > [service-name] > copy the URL from the service details page (e.g., service-name-hash.region.run.app)"
  type        = string
}

variable "github_token" {
  description = "GitHub Personal Access Token with repo scope for managing repository secrets"
  type        = string
  sensitive   = true
}
