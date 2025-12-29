locals {
  function_name = "synthetic-meat-ingestor"
}

# 1. Archive the Cloud Function source code from the 'ingestion' directory.
data "archive_file" "synthetic_meat_source" {
  type        = "zip"
  source_dir  = "${path.module}/../ingestion/synthetic-meat"
  output_path = "${path.module}/../dist/${local.function_name}.zip"

  # Exclude files not needed for the function runtime
  excludes = [
    "tests",
    ".gitignore",
    "__pycache__",
    ".pytest_cache",
    "uv.lock"
  ]
}

# 2. Upload the zipped source code to the 'deps' GCS bucket.
#    A new object is created each time the source code MD5 hash changes.
resource "google_storage_bucket_object" "synthetic_meat_source" {
  name   = "source/${local.function_name}-${data.archive_file.synthetic_meat_source.output_md5}.zip"
  bucket = google_storage_bucket.deps.name
  source = data.archive_file.synthetic_meat_source.output_path
}

# 3. Define the Cloud Function (2nd Gen).
resource "google_cloudfunctions2_function" "ingestor" {
  project  = var.project_id
  name     = local.function_name
  location = var.region

  build_config {
    runtime     = "python311"
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
      BRONZE_BUCKET = google_storage_bucket.bronze.name
    }
  }

  depends_on = [
    google_project_service.apis,
  ]
}

# 4. Create a dedicated service account for the Cloud Scheduler job.
resource "google_service_account" "scheduler_sa" {
  project      = var.project_id
  account_id   = "scheduler-cf-invoker"
  display_name = "Cloud Scheduler CF Invoker"
  description  = "Service account for Cloud Scheduler to invoke Cloud Functions"
}

# 5. Grant the scheduler's service account the 'Cloud Run Invoker' role.
resource "google_cloudfunctions2_function_iam_member" "invoker" {
  project        = google_cloudfunctions2_function.ingestor.project
  location       = google_cloudfunctions2_function.ingestor.location
  cloud_function = google_cloudfunctions2_function.ingestor.name
  role           = "roles/run.invoker"
  member         = "serviceAccount:${google_service_account.scheduler_sa.email}"
}

# 6. Create the Cloud Scheduler job to trigger the function daily.
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
