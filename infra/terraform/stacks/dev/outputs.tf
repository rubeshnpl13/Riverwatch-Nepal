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

output "lake_bucket_name" {
  description = "RiverWatch Cloud Storage data lake bucket."
  value       = module.data_lake.bucket_name
}


output "lake_bucket_uri" {
  description = "RiverWatch Cloud Storage data lake URI."
  value       = module.data_lake.bucket_uri
}


output "lake_managed_folders" {
  description = "Managed data lake folder paths."
  value       = module.data_lake.managed_folders
}

output "event_topic_names" {
  description = "RiverWatch event Pub/Sub topic names."
  value       = module.event_bus.topic_names
}


output "worker_subscription_names" {
  description = "RiverWatch Pub/Sub worker subscription names."
  value       = module.event_bus.worker_subscription_names
}


output "dead_letter_topic_ids" {
  description = "RiverWatch Pub/Sub dead-letter topic IDs."
  value       = module.event_bus.dead_letter_topic_ids
}


output "dead_letter_subscription_ids" {
  description = "RiverWatch Pub/Sub dead-letter subscription IDs."
  value       = module.event_bus.dead_letter_subscription_ids
}


output "processing_completed_audit_subscription_id" {
  description = "Subscription retaining processing.completed events."

  value = (
    module.event_bus
    .processing_completed_audit_subscription_id
  )
}
output "analytics_dataset_id" {
  description = "RiverWatch BigQuery analytics dataset."
  value       = module.analytics.dataset_id
}


output "analytics_connection_name" {
  description = "RiverWatch BigQuery lake connection."
  value       = module.analytics.connection_name
}


output "analytics_connection_service_account_id" {
  description = "Service account used for delegated lake access."

  value = (
    module.analytics
    .connection_service_account_id
  )
}

output "stations_external_table_id" {
  description = (
    "BigLake external station table ID."
  )

  value = (
    google_bigquery_table
    .stations_external
    .id
  )
}


output "observations_external_table_id" {
  description = (
    "BigLake external observation table ID."
  )

  value = (
    google_bigquery_table
    .observations_external
    .id
  )
}


output "stations_table_id" {
  description = (
    "Canonical RiverWatch stations view ID."
  )

  value = (
    google_bigquery_table
    .stations
    .id
  )
}


output "observations_table_id" {
  description = (
    "Canonical RiverWatch observations view ID."
  )

  value = (
    google_bigquery_table
    .observations
    .id
  )
}