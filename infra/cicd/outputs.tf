output "workload_identity_pool_name" {
  value = module.cicd.workload_identity_pool_name
}

output "workload_identity_provider_name" {
  value = module.cicd.workload_identity_provider_name
}

output "impersonated_service_account" {
  value = module.cicd.impersonated_service_account
}

output "admin_service_account" {
  value = module.cicd.admin_service_account
}
