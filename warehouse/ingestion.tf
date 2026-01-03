locals {
  ingestion_service_name = "synthetic-meat-ingestor"
  ingestion_service_url = google_cloud_run_v2_service.ingestor.uri
}

# Deploys the synthetic meat data generator as a Cloud Run service.
resource "google_cloud_run_v2_service" "ingestor" {
  project  = var.project_id
  name     = local.service_name
  location = var.app_engine_region

  # Allow the service to be deleted easily in dev environments.
  deletion_protection = false

  template {
    service_account = google_service_account.ingestion_sa.email
    containers {
      image = var.image_uri
      env {
        name  = "BRONZE_BUCKET"
        value = google_storage_bucket.bronze_bucket.name
      }
    }
  }

}
