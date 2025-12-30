locals {
  service_name = "synthetic-meat-ingestor"
}

# Deploys the synthetic meat data generator as a Cloud Run service.
resource "google_cloud_run_v2_service" "ingestor" {
  project  = var.project_id
  name     = local.service_name
  location = var.region

  # Allow the service to be deleted easily in dev environments.
  deletion_protection = false

  template {
    service_account = google_service_account.ingestion_sa.email
    containers {
      image = var.image_uri
      env {
        name  = "BRONZE_BUCKET"
        value = google_storage_bucket.bronze.name
      }
    }
  }

  depends_on = [google_artifact_registry_repository.images]
}
