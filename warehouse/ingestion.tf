locals {
  service_name = "synthetic-meat-ingestor"
}

# 1. Define the Cloud Run service.
resource "google_cloud_run_v2_service" "ingestor" {
  project  = var.project_id
  name     = local.service_name
  location = var.region

  template {
    service_account = google_service_account.ingestion_sa.email
    containers {
      image = var.image_uri
      ports {
        container_port = 8080
      }
      env {
        name  = "BRONZE_BUCKET"
        value = google_storage_bucket.bronze.name
      }
    }
  }

  depends_on = [
    google_project_service.apis,
    # Ensure repo exists and CI/CD SA can write to it before this runs
    google_artifact_registry_repository.docker_images,
  ]
}

# 2. Create a dedicated service account for the Cloud Scheduler job.
resource "google_service_account" "scheduler_sa" {
  project      = var.project_id
  account_id   = "scheduler-cr-invoker"
  display_name = "Cloud Scheduler CR Invoker"
  description  = "Service account for Cloud Scheduler to invoke Cloud Run services"
}

# 3. Grant the scheduler's service account the 'Cloud Run Invoker' role.
resource "google_cloud_run_v2_service_iam_member" "invoker" {
  project  = google_cloud_run_v2_service.ingestor.project
  location = google_cloud_run_v2_service.ingestor.location
  name     = google_cloud_run_v2_service.ingestor.name
  role     = "roles/run.invoker"
  member   = "serviceAccount:${google_service_account.scheduler_sa.email}"
}

# 4. Create the Cloud Scheduler job to trigger the service daily.
resource "google_cloud_scheduler_job" "ingestor_trigger" {
  project     = var.project_id
  name        = "${local.service_name}-trigger"
  region      = var.app_engine_region
  description = "Triggers the synthetic meat data generation service daily."
  schedule    = "0 5 * * *" # Daily at 5 AM UTC
  time_zone   = "Etc/UTC"

  http_target {
    uri         = google_cloud_run_v2_service.ingestor.uri
    http_method = "POST"
    headers = {
      "Content-Type" = "application/json"
    }
    # Body is empty for daily runs (defaults to yesterday's date).
    # For backfills, you can send: {"target_date": "YYYY-MM-DD"}
    body = base64encode("{}")

    oidc_token {
      service_account_email = google_service_account.scheduler_sa.email
      audience              = google_cloud_run_v2_service.ingestor.uri
    }
  }

  depends_on = [
    google_cloud_run_v2_service_iam_member.invoker,
    google_app_engine_application.app,
  ]
}
