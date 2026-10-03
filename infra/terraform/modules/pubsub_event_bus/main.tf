locals {
  event_topics = toset([
    "ingestion-requested",
    "ingestion-completed",
    "processing-completed",
  ])

  dead_letter_topics = toset([
    "ingestion-requested",
    "ingestion-completed",
  ])

  worker_subscriptions = {
    ingestion = {
      name             = "ingestion-worker"
      topic            = "ingestion-requested"
      dead_letter_topic = "ingestion-requested"
    }

    processing = {
      name             = "processing-worker"
      topic            = "ingestion-completed"
      dead_letter_topic = "ingestion-completed"
    }
  }
}


resource "google_pubsub_topic" "event" {
  for_each = local.event_topics

  project = var.project_id
  name    = "${var.name_prefix}-${each.value}"

  labels = var.labels
}


resource "google_pubsub_topic" "dead_letter" {
  for_each = local.dead_letter_topics

  project = var.project_id
  name = (
    "${var.name_prefix}-${each.value}-dlq"
  )

  labels = var.labels
}


resource "google_pubsub_subscription" "worker" {
  for_each = local.worker_subscriptions

  project = var.project_id
  name = (
    "${var.name_prefix}-${each.value.name}"
  )

  topic = (
    google_pubsub_topic.event[
      each.value.topic
    ].id
  )

  ack_deadline_seconds = (
    var.ack_deadline_seconds
  )

  message_retention_duration = (
    var.message_retention_duration
  )

  retain_acked_messages = false

  expiration_policy {
    ttl = ""
  }

  retry_policy {
    minimum_backoff = (
      var.minimum_backoff
    )

    maximum_backoff = (
      var.maximum_backoff
    )
  }

  dead_letter_policy {
    dead_letter_topic = (
      google_pubsub_topic.dead_letter[
        each.value.dead_letter_topic
      ].id
    )

    max_delivery_attempts = (
      var.max_delivery_attempts
    )
  }

  labels = var.labels
}


resource "google_pubsub_subscription" "dead_letter" {
  for_each = local.dead_letter_topics

  project = var.project_id

  name = (
    "${var.name_prefix}-${each.value}-dlq"
  )

  topic = (
    google_pubsub_topic.dead_letter[
      each.value
    ].id
  )

  ack_deadline_seconds = (
    var.ack_deadline_seconds
  )

  message_retention_duration = (
    var.message_retention_duration
  )

  retain_acked_messages = false

  expiration_policy {
    ttl = ""
  }

  labels = var.labels
}


resource "google_pubsub_subscription" "processing_completed_audit" {
  project = var.project_id

  name = (
    "${var.name_prefix}-processing-completed-audit"
  )

  topic = (
    google_pubsub_topic.event[
      "processing-completed"
    ].id
  )

  ack_deadline_seconds = (
    var.ack_deadline_seconds
  )

  message_retention_duration = (
    var.message_retention_duration
  )

  retain_acked_messages = false

  expiration_policy {
    ttl = ""
  }

  labels = var.labels
}