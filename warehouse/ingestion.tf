locals {
  service_name = "synthetic-meat-ingestor"
}

# Grant the Scheduler SA permission to invoke the Cloud Run service.
resource "google_cloud_run_v2_service_iam_member" "scheduler_invoker" {
  project  = google_cloud_run_v2_service.ingestor.project
  location = google_cloud_run_v2_service.ingestor.location
  name     = google_cloud_run_v2_service.ingestor.name
  role     = "roles/run.invoker"
  member   = "serviceAccount:${google_service_account.scheduler_sa.email}"
}

# Cloud Scheduler job to trigger the ingestion service daily.
resource "google_cloud_scheduler_job" "ingestor_trigger" {
  project     = var.project_id
  name        = "daily-synthetic-meat-ingestion"
  description = "Triggers the synthetic meat data generator Cloud Run service."
  schedule    = "0 5 * * *" # Daily at 5:00 AM UTC
  time_zone   = "Etc/UTC"
  region      = var.app_engine_region

  http_target {
    uri         = google_cloud_run_v2_service.ingestor.uri
    http_method = "GET"

    oidc_token {
      service_account_email = google_service_account.scheduler_sa.email
    }
  }

  # Ensure the service and permissions are created first.
  depends_on = [
    google_cloud_run_v2_service_iam_member.scheduler_invoker,
  ]
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

}
