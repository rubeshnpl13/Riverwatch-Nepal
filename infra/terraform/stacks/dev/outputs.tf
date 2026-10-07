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

output "station_observation_history_view_id" {
  description = (
    "Canonical station observation history view ID."
  )

  value = (
    google_bigquery_table
    .station_observation_history
    .id
  )
}


output "current_river_snapshot_view_id" {
  description = (
    "Canonical current river snapshot view ID."
  )

  value = (
    google_bigquery_table
    .current_river_snapshot
    .id
  )
}


output "basin_current_summary_view_id" {
  description = (
    "Canonical basin current summary view ID."
  )

  value = (
    google_bigquery_table
    .basin_current_summary
    .id
  )
}


output "current_network_summary_view_id" {
  description = (
    "Canonical current network summary view ID."
  )

  value = (
    google_bigquery_table
    .current_network_summary
    .id
  )
}

output "container_repository_name" {
  description = "RiverWatch Artifact Registry repository."
  value       = module.container_registry.repository_name
}


output "container_repository_uri" {
  description = "RiverWatch Docker repository URI."
  value       = module.container_registry.docker_repository_uri
}


output "scheduled_ingestion_job_name" {
  description = "Cloud Scheduler ingestion job name when scheduling is enabled."
  value       = module.scheduled_ingestion.job_name
}


output "scheduled_ingestion_trigger_payload" {
  description = "Scheduled ingestion transport contract."
  value       = module.scheduled_ingestion.trigger_payload
}


output "ingestion_event_service_name" {
  description = "Cloud Run ingestion event service name when enabled."

  value = (
    module.ingestion_event_service.name
  )
}


output "ingestion_event_service_uri" {
  description = "Cloud Run ingestion event service URI when enabled."

  value = (
    module.ingestion_event_service.uri
  )
}


output "processing_event_service_name" {
  description = "Cloud Run processing event service name when enabled."

  value = (
    module.processing_event_service.name
  )
}


output "processing_event_service_uri" {
  description = "Cloud Run processing event service URI when enabled."

  value = (
    module.processing_event_service.uri
  )
}

output "processing_gateway_service_account_email" {
  description = (
    "Cloud Run processing gateway identity."
  )

  value = (
    module.runtime_service_accounts.emails[
      "processing_gateway"
    ]
  )
}


output "processing_runtime_service_account_email" {
  description = (
    "Managed Spark workload identity."
  )

  value = (
    module.runtime_service_accounts.emails[
      "processing"
    ]
  )
}


output "dataproc_runtime_version" {
  description = (
    "Pinned Managed Spark runtime version."
  )

  value = (
    var.dataproc_runtime_version
  )
}


output "processing_spark_image" {
  description = (
    "Managed Spark custom container image contract."
  )

  value = (
    local.processing_spark_image
  )
}


output "api_service_name" {
  description = "RiverWatch FastAPI Cloud Run service name when enabled."
  value       = module.api_service.name
}


output "api_service_uri" {
  description = "RiverWatch FastAPI public HTTPS URI when enabled."
  value       = module.api_service.uri
}


output "api_image" {
  description = "FastAPI container image contract."
  value       = local.api_image
}