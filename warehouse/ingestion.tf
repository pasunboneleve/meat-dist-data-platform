locals {
  ingestion_service_name = "synthetic-meat-ingestor"
  ingestion_service_url  = google_cloud_run_v2_service.ingestor.uri
}

# Deploys the synthetic meat data generator as a Cloud Run service.
resource "google_cloud_run_v2_service" "ingestor" {
  project  = var.project_id
  name     = local.ingestion_service_name
  location = var.app_engine_region

  # Allow the service to be deleted easily in dev environments.
  deletion_protection = false

  template {
    service_account = google_service_account.ingestion_sa.email
    containers {
      image = var.image_uri
      resources {
        limits = {
          memory = "1Gi"
        }
      }
      env {
        name  = "BRONZE_BUCKET"
        value = google_storage_bucket.bronze_bucket.name
      }
    }
  }

}

# Allow Composer service account (dataproc_sa) to invoke this specific Cloud Run service
resource "google_cloud_run_v2_service_iam_member" "dataproc_sa_invoker" {
  project  = google_cloud_run_v2_service.ingestor.project
  location = google_cloud_run_v2_service.ingestor.location
  name     = google_cloud_run_v2_service.ingestor.name
  role     = "roles/run.invoker"
  member   = "serviceAccount:${google_service_account.dataproc_sa.email}"
}

resource "google_cloud_run_v2_service_iam_member" "dataproc_serverless_invoker" {
  project  = google_cloud_run_v2_service.ingestor.project
  location = google_cloud_run_v2_service.ingestor.location
  name     = google_cloud_run_v2_service.ingestor.name
  role     = "roles/run.invoker"
  member   = "serviceAccount:${google_service_account.dataproc_sa.email}"
}

resource "google_project_iam_member" "cloud_run_log_writer" {
  project = var.project_id
  role    = "roles/logging.logWriter"
  member  = "serviceAccount:${google_service_account.ingestion_sa.email}"

  depends_on = [google_service_account.ingestion_sa]
}
