terraform {
  backend "gcs" {
    # Configure with -backend-config at init time.
    # The CI/CD pipeline will provide the bucket and prefix.
  }
}
