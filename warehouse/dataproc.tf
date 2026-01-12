# 1. Create custom service account for Dataproc Serverless batches
resource "google_service_account" "dataproc_batch_sa" {
  account_id   = "dataproc-batch-sa"
  display_name = "Dataproc Serverless Batch Service Account"
  description  = "Dedicated SA for Dataproc Serverless Spark batches run from Composer"
}

# 2. Grant necessary roles to the batch SA
resource "google_project_iam_member" "dataproc_worker" {
  project = var.project_id
  role    = "roles/dataproc.worker"
  member  = "serviceAccount:${google_service_account.dataproc_batch_sa.email}"
}

resource "google_project_iam_member" "storage_access" {
  project = var.project_id
  role    = "roles/storage.objectAdmin"
  member  = "serviceAccount:${google_service_account.dataproc_batch_sa.email}"
}

# Add if using BigLake Metastore / Iceberg REST catalog
resource "google_project_iam_member" "biglake_access" {
  project = var.project_id
  role    = "roles/biglake.editor"
  member  = "serviceAccount:${google_service_account.dataproc_batch_sa.email}"
}

resource "google_project_iam_member" "bigquery_access" {
  project = var.project_id
  role    = "roles/bigquery.dataEditor"
  member  = "serviceAccount:${google_service_account.dataproc_batch_sa.email}"
}

resource "google_service_account_iam_member" "oauth_access" {
  service_account_id = google_service_account.dataproc_batch_sa.name
  role    = "roles/iam.serviceAccountTokenCreator"
  member  = "serviceAccount:${google_service_account.dataproc_batch_sa.email}"
}

# 3. Allow Composer's runtime SA to impersonate (act as) this custom SA
resource "google_service_account_iam_member" "composer_act_as_batch_sa" {
  service_account_id = google_service_account.dataproc_batch_sa.name
  role               = "roles/iam.serviceAccountUser"
  member             = "serviceAccount:${google_service_account.dataproc_sa.email}"
}

resource "google_project_iam_custom_role" "biglake_rest_consumer" {
  role_id     = "BigLakeRestConsumer"
  title       = "BigLake REST Catalog Consumer"
  description = "Minimal permissions for Iceberg REST catalog via BigLake"
  permissions = [
    "serviceusage.services.use",
  ]
}

resource "google_project_iam_member" "custom_rest_consumer" {
  project = var.project_id
  role    = "projects/${var.project_id}/roles/BigLakeRestConsumer"
  member  = "serviceAccount:${google_service_account.dataproc_batch_sa.email}"
}
