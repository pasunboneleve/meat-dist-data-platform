#!/usr/bin/env bash
set -euo pipefail

# Bootstrap a GCS bucket for Terraform/OpenTofu state with best-practices.
# Usage:
#   GCP_PROJECT_ID=<id> GCS_BUCKET=<name> ./scripts/bootstrap-tf-state.sh
# Optional: GCP_REGION (defaults to australia-southeast1), RETENTION_DAYS (defaults to 30)

: "${GCP_PROJECT_ID:?Set GCP_PROJECT_ID}"
: "${GCS_BUCKET:?Set GCS_BUCKET}"
LOCATION=${LOCATION:-australia-southeast1}
RETENTION_DAYS=${RETENTION_DAYS:-30}

echo "Creating bucket gs://${GCS_BUCKET} in ${GCP_REGION} (if not exists)"
if ! gsutil ls -b gs://${GCS_BUCKET} >/dev/null 2>&1; then
  gsutil mb -p ${GCP_PROJECT_ID} -l ${GCP_REGION} -b on gs://${GCS_BUCKET}
fi

echo "Enabling uniform bucket-level access"
gsutil uniformbucketlevelaccess set on gs://${GCS_BUCKET}

echo "Enabling versioning"
gsutil versioning set on gs://${GCS_BUCKET}

echo "Setting retention policy: ${RETENTION_DAYS} days"
gsutil retention set ${RETENTION_DAYS}d gs://${GCS_BUCKET}

echo "Done. Use this backend config: bucket=${GCS_BUCKET}, prefix=${GCP_PROJECT_ID}/infra"
