locals {
  service_name = "synthetic-meat-ingestor"
}

# Re-define the Cloud Run service to disable deletion protection.
# This is a temporary step to allow for a clean removal.
resource "google_cloud_run_v2_service" "ingestor" {
  project  = var.project_id
  name     = local.service_name
  location = var.region

  # Set deletion_protection to false to allow the next apply to destroy it.
  deletion_protection = false

  # The template must be specified, but its values are ignored during this update.
  template {
    service_account = google_service_account.ingestion_sa.email
    containers {
      image = var.image_uri
    }
  }
}
