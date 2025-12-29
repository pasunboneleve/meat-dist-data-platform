# Infrastructure (OpenTofu/Terraform)

This folder provisions the infrastructure for deploying **GitHub
Actions**. You don't need to change anything in this directory apart
from providing configurations for its `tfvars` files and running
`tofu apply` once at when initialising the project. After that, when
you add changes to `warehouse`, the changes will be applied by
**GitHub Actions** automagically.

This folder provisions:
- Workload Identity Pool and Provider for GitHub OIDC
- IAM binding to let your GitHub repo impersonate the deploy service account

## Prereqs
- gcloud (authenticated to the target project)
- Terraform/OpenTofu 1.5+

## 1) Create a remote state bucket (one-time)
```bash
export GCP_PROJECT_ID=<your-project-id>
export GCS_BUCKET=<globally-unique-bucket-name>
./scripts/bootstrap-tf-state.sh
```

## 2) Init with GCS backend
```bash
cd infra
terraform init \
  -backend-config="bucket=$GCS_BUCKET" \
  -backend-config="prefix=gcp-rust-blog/infra"
```

## 3) Apply
Set values matching your environment:
```bash
terraform apply \
  -var="project_id=<GCP_PROJECT_ID>" \
  -var="project_number=<PROJECT_NUMBER>" \
  -var="pool_id=github-pool" \
  -var="provider_id=github-provider" \
  -var="github_owner=<GITHUB_OWNER>" \
  -var="github_repo=<GITHUB_REPO>"
# Note: service_account_email is automatically constructed from project_id
```

Outputs will include the WIF resource names.
