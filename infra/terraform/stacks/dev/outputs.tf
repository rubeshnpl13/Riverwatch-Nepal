output "project_id" {
  description = "Configured GCP project ID."
  value       = var.project_id
}


output "region" {
  description = "Configured primary GCP region."
  value       = var.region
}


output "environment" {
  description = "Configured RiverWatch environment."
  value       = var.environment
}


output "name_prefix" {
  description = "Common RiverWatch resource name prefix."
  value       = local.name_prefix
}


output "common_labels" {
  description = "Common labels for RiverWatch resources."
  value       = local.common_labels
}

output "enabled_services" {
  description = "Google Cloud APIs managed for RiverWatch."
  value       = module.project_services.enabled_services
}


output "runtime_service_account_emails" {
  description = "RiverWatch runtime service account emails."
  value       = module.runtime_service_accounts.emails
}


output "runtime_service_account_members" {
  description = "RiverWatch runtime IAM member identifiers."
  value       = module.runtime_service_accounts.members
}