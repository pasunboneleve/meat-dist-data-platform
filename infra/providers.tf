provider "google" {
  project = var.project_id
}

provider "github" {
  owner = var.github_owner
  token = var.github_token
}
