locals {
  function_name = "synthetic-meat-ingestor"
}

# 1. Upload the zipped source code (created by CI) to the 'deps' GCS bucket.
#    A new object is created each time the source code MD5 hash changes.
resource "google_storage_bucket_object" "synthetic_meat_source" {
  name   = "source/${local.function_name}-${filemd5("${path.module}/../dist/${local.function_name}.zip")}.zip"
  bucket = google_storage_bucket.deps.name
  source = "${path.module}/../dist/${local.function_name}.zip"
}

# 2. Define the Cloud Function (2nd Gen).
resource "google_cloudfunctions2_function" "ingestor" {
  project  = var.project_id
  name     = local.function_name
  location = var.region

  build_config {
    runtime     = "python311"
    # The `entry_point` must be the name of the Python function to execute.
    entry_point = "generate_and_upload"
    source {
      storage_source {
        bucket = google_storage_bucket_object.synthetic_meat_source.bucket
        object = google_storage_bucket_object.synthetic_meat_source.name
      }
    }
  }

  service_config {
    max_instance_count             = 1
    min_instance_count             = 0
    available_memory               = "512Mi"
    timeout_seconds                = 300
    service_account_email          = google_service_account.cloud_function_sa.email
    all_traffic_on_latest_revision = true
    environment_variables = {
      BRONZE_BUCKET   = google_storage_bucket.bronze.name
      # The `FUNCTION_SOURCE` variable tells the Functions Framework which file
      # contains the entry point function, which is necessary for a `src` layout.
      FUNCTION_SOURCE = "src/synthesise.py"
    }
  }

  depends_on = [
    google_project_service.apis,
  ]
}

# 3. Create a dedicated service account for the Cloud Scheduler job.
resource "google_service_account" "scheduler_sa" {
  project      = var.project_id
  account_id   = "scheduler-cf-invoker"
  display_name = "Cloud Scheduler CF Invoker"
  description  = "Service account for Cloud Scheduler to invoke Cloud Functions"
}

# 4. Grant the scheduler's service account the 'Cloud Run Invoker' role.
resource "google_cloudfunctions2_function_iam_member" "invoker" {
  project        = google_cloudfunctions2_function.ingestor.project
  location       = google_cloudfunctions2_function.ingestor.location
  cloud_function = google_cloudfunctions2_function.ingestor.name
  role           = "roles/run.invoker"
  member         = "serviceAccount:${google_service_account.scheduler_sa.email}"
}

# 5. Create the Cloud Scheduler job to trigger the function daily.
resource "google_cloud_scheduler_job" "ingestor_trigger" {
  project     = var.project_id
  name        = "${local.function_name}-trigger"
  region      = var.region
  description = "Triggers the synthetic meat data generation function daily."
  schedule    = "0 5 * * *" # Daily at 5 AM UTC
  time_zone   = "Etc/UTC"

  http_target {
    uri         = google_cloudfunctions2_function.ingestor.service_config[0].uri
    http_method = "POST"
    headers = {
      "Content-Type" = "application/json"
    }
    # Body is empty for daily runs (defaults to yesterday's date).
    # For backfills, you can send: {"target_date": "YYYY-MM-DD"}
    body = base64encode("{}")

    oidc_token {
      service_account_email = google_service_account.scheduler_sa.email
    }
  }

  depends_on = [
    google_cloudfunctions2_function_iam_member.invoker,
  ]
}
