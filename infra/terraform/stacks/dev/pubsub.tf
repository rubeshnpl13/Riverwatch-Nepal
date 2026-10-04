data "google_project" "current" {
  project_id = var.project_id
}


module "event_bus" {
  source = "../../modules/pubsub_event_bus"

  project_id  = var.project_id
  name_prefix = local.name_prefix
  labels      = local.common_labels

  message_retention_duration = "604800s"

  ack_deadline_seconds = (
    var.enable_cloud_run_event_services
    ? 600
    : 60
  )

  minimum_backoff = "10s"
  maximum_backoff = "300s"

  # Pub/Sub requires at least five attempts
  # when a dead-letter policy is configured.
  max_delivery_attempts = 5

  worker_push_configs = (
    var.enable_cloud_run_event_services
    ? {
      ingestion = {
        push_endpoint = (
          "${module.ingestion_event_service.uri}/events/pubsub"
        )

        service_account_email = (
          module.runtime_service_accounts.emails[
            "event_invoker"
          ]
        )

        audience = (
          module.ingestion_event_service.uri
        )
      }

      processing = {
        push_endpoint = (
          "${module.processing_event_service.uri}/events/pubsub"
        )

        service_account_email = (
          module.runtime_service_accounts.emails[
            "event_invoker"
          ]
        )

        audience = (
          module.processing_event_service.uri
        )
      }
    }
    : {}
  )

  depends_on = [
    module.project_services,
  ]
}


locals {
  pubsub_service_agent_member = (
    "serviceAccount:service-${data.google_project.current.number}@gcp-sa-pubsub.iam.gserviceaccount.com"
  )
}


resource "google_service_account_iam_member" "pubsub_event_invoker_token_creator" {
  service_account_id = (
    module.runtime_service_accounts.names[
      "event_invoker"
    ]
  )

  role = "roles/iam.serviceAccountTokenCreator"

  member = (
    local.pubsub_service_agent_member
  )
}


resource "google_pubsub_subscription_iam_member" "ingestion_consumer" {
  count = (
    var.enable_cloud_run_event_services
    ? 0
    : 1
  )

  project = var.project_id

  subscription = (
    module.event_bus
    .worker_subscription_ids[
      "ingestion"
    ]
  )

  role = "roles/pubsub.subscriber"

  member = (
    module.runtime_service_accounts.members[
      "ingestion"
    ]
  )
}


resource "google_pubsub_topic_iam_member" "ingestion_completion_publisher" {
  project = var.project_id

  topic = (
    module.event_bus.topic_ids[
      "ingestion-completed"
    ]
  )

  role = "roles/pubsub.publisher"

  member = (
    module.runtime_service_accounts.members[
      "ingestion"
    ]
  )
}


resource "google_pubsub_subscription_iam_member" "processing_consumer" {
  count = (
    var.enable_cloud_run_event_services
    ? 0
    : 1
  )

  project = var.project_id

  subscription = (
    module.event_bus
    .worker_subscription_ids[
      "processing"
    ]
  )

  role = "roles/pubsub.subscriber"

  member = (
    module.runtime_service_accounts.members[
      "processing"
    ]
  )
}


resource "google_pubsub_topic_iam_member" "processing_completion_publisher" {
  project = var.project_id

  topic = (
    module.event_bus.topic_ids[
      "processing-completed"
    ]
  )

  role = "roles/pubsub.publisher"

  member = (
    module.runtime_service_accounts.members[
      "processing"
    ]
  )
}


resource "google_pubsub_topic_iam_member" "dead_letter_publisher" {
  for_each = (
    module.event_bus
    .dead_letter_topic_ids
  )

  project = var.project_id
  topic   = each.value

  role = "roles/pubsub.publisher"

  member = (
    local.pubsub_service_agent_member
  )
}


resource "google_pubsub_subscription_iam_member" "dead_letter_acknowledger" {
  for_each = (
    module.event_bus
    .worker_subscription_ids
  )

  project      = var.project_id
  subscription = each.value

  role = "roles/pubsub.subscriber"

  member = (
    local.pubsub_service_agent_member
  )
}