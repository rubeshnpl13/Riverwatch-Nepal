output "topic_ids" {
  description = "RiverWatch event Pub/Sub topic IDs."

  value = {
    for key, topic in google_pubsub_topic.event :
    key => topic.id
  }
}


output "topic_names" {
  description = "RiverWatch event Pub/Sub topic names."

  value = {
    for key, topic in google_pubsub_topic.event :
    key => topic.name
  }
}


output "worker_subscription_ids" {
  description = "RiverWatch worker subscription IDs."

  value = {
    for key, subscription in google_pubsub_subscription.worker :
    key => subscription.id
  }
}


output "worker_subscription_names" {
  description = "RiverWatch worker subscription names."

  value = {
    for key, subscription in google_pubsub_subscription.worker :
    key => subscription.name
  }
}


output "dead_letter_topic_ids" {
  description = "RiverWatch dead-letter topic IDs."

  value = {
    for key, topic in google_pubsub_topic.dead_letter :
    key => topic.id
  }
}


output "dead_letter_subscription_ids" {
  description = "RiverWatch dead-letter subscription IDs."

  value = {
    for key, subscription in google_pubsub_subscription.dead_letter :
    key => subscription.id
  }
}


output "processing_completed_audit_subscription_id" {
  description = "Subscription retaining processing.completed events."

  value = (
    google_pubsub_subscription
    .processing_completed_audit
    .id
  )
}