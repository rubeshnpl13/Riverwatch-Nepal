locals {
  trigger_payload = {
    schema_version = 1
    message_kind   = "scheduled_ingestion_trigger"
    provider       = var.provider_name
    endpoint       = var.endpoint
  }
}


resource "google_cloud_scheduler_job" "this" {
  count       = var.enabled ? 1 : 0
  project     = var.project_id
  region      = var.region
  name        = var.name
  description = "Publish the RiverWatch scheduled ingestion trigger."
  schedule    = var.schedule
  time_zone   = var.time_zone

  pubsub_target {
    topic_name = var.topic_id

    data = base64encode(
      jsonencode(local.trigger_payload)
    )

    attributes = {
      riverwatch_contract = "scheduled-ingestion-trigger-v1"
    }
  }
}