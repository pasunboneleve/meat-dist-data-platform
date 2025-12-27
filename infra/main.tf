module "cicd" {
  source = "./cicd"

  gcp_owner      = var.gcp_owner
  repository_id  = var.repository_id
  project_id     = var.project_id
  project_number = var.project_number
  region         = var.region
  pool_id        = var.pool_id
  provider_id    = var.provider_id
  github_owner   = var.github_owner
  github_repo    = var.github_repo
  cloud_run_url  = var.cloud_run_url
  github_token   = var.github_token
}

module "datawarehouse" {
  source = "./datawarehouse"

  project_id = var.project_id
  region     = var.region
}

# Enable required Google Cloud APIs
resource "google_project_service" "required_apis" {
  project  = var.project_id
  service  = "serviceusage.googleapis.com" # Enable Service Usage API first
  disable_on_destroy = false
}

resource "google_project_service" "apis" {
  depends_on = [google_project_service.required_apis]
  project = var.project_id
  for_each = toset([
    "run.googleapis.com",
    "artifactregistry.googleapis.com",
    "iam.googleapis.com",
    "iamcredentials.googleapis.com",
    "dns.googleapis.com",
    "cloudresourcemanager.googleapis.com",
    "compute.googleapis.com",
    "bigquery.googleapis.com",
    "dataplex.googleapis.com",
    "storage.googleapis.com"
  ])
  service            = each.value
  disable_on_destroy = false
}
