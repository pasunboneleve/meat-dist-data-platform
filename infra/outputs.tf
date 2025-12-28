output "workload_identity_pool_name" {
  value = local.wif_pool_name
}

output "workload_identity_provider_name" {
  value = local.wif_provider_name
}

output "impersonated_service_account" {
  value = google_service_account.github_actions.email
}

output "admin_service_account" {
  value = google_service_account.admin.email
}
