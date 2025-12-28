# GitHub Repository Secrets
# These secrets will be automatically configured for GitHub Actions

resource "github_actions_secret" "gcp_project_id" {
  repository      = var.github_repo
  secret_name     = "GCP_PROJECT_ID"
  plaintext_value = var.project_id
}

resource "github_actions_secret" "gcp_project_number" {
  repository      = var.github_repo
  secret_name     = "GCP_PROJECT_NUMBER"
  plaintext_value = var.project_number
}

resource "github_actions_secret" "gcp_region" {
  repository      = var.github_repo
  secret_name     = "GCP_REGION"
  plaintext_value = var.region
}

resource "github_actions_secret" "gcp_workload_identity_pool" {
  repository      = var.github_repo
  secret_name     = "GCP_WORKLOAD_IDENTITY_POOL"
  plaintext_value = var.pool_id
}

resource "github_actions_secret" "gcp_workload_identity_provider" {
  repository      = var.github_repo
  secret_name     = "GCP_WORKLOAD_IDENTITY_PROVIDER"
  plaintext_value = var.provider_id
}

resource "github_actions_secret" "gcp_service_account" {
  repository      = var.github_repo
  secret_name     = "GCP_SERVICE_ACCOUNT"
  plaintext_value = google_service_account.github_actions.email
}

resource "github_actions_secret" "cloud_run_service_url" {
  repository      = var.github_repo
  secret_name     = "CLOUD_RUN_SERVICE_URL"
  plaintext_value = var.cloud_run_url
}