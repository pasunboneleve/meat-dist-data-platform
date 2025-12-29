locals {
  # Roles the deploy SA needs at the project level
  sa_roles = [
    "roles/editor",
    "roles/serviceusage.serviceUsageAdmin",
    "roles/resourcemanager.projectIamAdmin", # Allows the SA to grant IAM roles
    "roles/artifactregistry.writer",       # To upload packages to Artifact Registry
    "roles/artifactregistry.admin",        # To set IAM policies on repositories
    "roles/run.admin",                     # To set IAM policies on Cloud Run services
  ]

  # Required APIs for the deployment pipeline
  required_apis = [
    "iam.googleapis.com",
    "iamcredentials.googleapis.com",
    "cloudresourcemanager.googleapis.com",
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

  depends_on = [google_project_service.apis]
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


