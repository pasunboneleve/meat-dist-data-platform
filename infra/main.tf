locals {
  # Roles the deploy SA needs at the project level
  sa_roles = [
    "roles/run.admin",
    "roles/iam.serviceAccountUser",
    "roles/artifactregistry.writer",
    "roles/serviceusage.serviceUsageAdmin",
    "roles/compute.loadBalancerAdmin",
  ]

  # Required APIs for the deployment pipeline
  required_apis = [
    "run.googleapis.com",
    "artifactregistry.googleapis.com",
    "iam.googleapis.com",
    "iamcredentials.googleapis.com",
    "dns.googleapis.com",
    "cloudresourcemanager.googleapis.com",
    "compute.googleapis.com",
    "bigquery.googleapis.com",
  ]

  # Full resource names
  wif_pool_name     = "projects/${var.project_number}/locations/global/workloadIdentityPools/${var.pool_id}"
  wif_provider_name = "${local.wif_pool_name}/providers/${var.provider_id}"

  # GitHub repository selector (owner/repo)
  github_repo_attr = "${var.github_owner}/${var.github_repo}"
}

# Enable required Google Cloud APIs
resource "google_project_service" "apis" {
  for_each = toset(local.required_apis)
  project  = var.project_id
  service  = each.value

  disable_dependent_services = false
  disable_on_destroy         = false
}

resource "google_iam_workload_identity_pool" "github" {
  project                   = var.project_id
  workload_identity_pool_id = var.pool_id
  display_name              = var.pool_id

  depends_on = [google_project_service.apis, google_project_service.bigquery]
}

resource "google_iam_workload_identity_pool_provider" "github" {
  project                            = var.project_id
  workload_identity_pool_id          = google_iam_workload_identity_pool.github.workload_identity_pool_id
  workload_identity_pool_provider_id = var.provider_id
  display_name                       = var.provider_id

  attribute_mapping = {
    "google.subject"       = "assertion.sub"
    "attribute.actor"      = "assertion.actor"
    "attribute.repository" = "assertion.repository"
    "attribute.ref"        = "assertion.ref"
    "attribute.workflow"   = "assertion.workflow"
    "attribute.aud"        = "assertion.aud"
  }

  attribute_condition = "attribute.repository == '${var.github_owner}/${var.github_repo}'"

  oidc {
    issuer_uri        = "https://token.actions.githubusercontent.com"
    allowed_audiences = ["sts.googleapis.com"]
  }
}

# Create the service account for GitHub Actions
resource "google_service_account" "github_actions" {
  project      = var.project_id
  account_id   = "github-actions-deploy"
  display_name = "GitHub Actions Deploy"
  description  = "Service account for GitHub Actions deployments"

  depends_on = [google_project_service.apis]
}

# Ensure BigQuery API is enabled
resource "google_project_service" "bigquery" {
  project = var.project_id
  service = "bigquery.googleapis.com"

  disable_dependent_services = false
  disable_on_destroy         = false
}

# Create administrative service account for organization-level tasks
resource "google_service_account" "admin" {
  project      = var.project_id
  account_id   = "infrastructure-admin"
  display_name = "Infrastructure Admin"
  description  = "Service account for administrative and organization policy tasks"

  depends_on = [google_project_service.apis]
}

# Create Artifact Registry repository
resource "google_artifact_registry_repository" "images" {
  project       = var.project_id
  location      = var.region
  repository_id = var.repository_id
  description   = "container images"
  format        = "DOCKER"

  depends_on = [google_project_service.apis]
}

# Create the service account for GitHub Actions
resource "google_service_account_iam_binding" "wif_impersonation" {
  service_account_id = google_service_account.github_actions.id
  role               = "roles/iam.workloadIdentityUser"
  members = [
    "principalSet://iam.googleapis.com/${local.wif_pool_name}/attribute.repository/${local.github_repo_attr}"
  ]
}

# Project-level roles for the deploy SA
resource "google_project_iam_member" "sa_roles" {
  for_each = toset(local.sa_roles)
  project  = var.project_id
  role     = each.value
  member   = "serviceAccount:${google_service_account.github_actions.email}"
}


# Allow owner to impersonate the admin service account
resource "google_service_account_iam_binding" "admin_impersonation" {
  service_account_id = google_service_account.admin.id
  role               = "roles/iam.serviceAccountTokenCreator"
  members = [
    "user:${var.gcp_owner}"
  ]
}

resource "google_service_account_iam_binding" "admin_user" {
  service_account_id = google_service_account.admin.id
  role               = "roles/iam.serviceAccountUser"
  members = [
    "user:${var.gcp_owner}"
  ]
}
