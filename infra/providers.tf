provider "google" {
  project = var.project_id
  region  = var.app_engine_region
}

provider "github" {
  owner = var.github_owner
  token = var.github_token
}
