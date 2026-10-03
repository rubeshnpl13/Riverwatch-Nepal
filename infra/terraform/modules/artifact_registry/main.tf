resource "google_artifact_registry_repository" "this" {
  project = var.project_id

  location      = var.location
  repository_id = var.repository_id

  description = (
    "RiverWatch container images."
  )

  format = "DOCKER"

  labels = var.labels

  docker_config {
    immutable_tags = true
  }

  cleanup_policy_dry_run = false

  cleanup_policies {
    id     = "delete-old-untagged"
    action = "DELETE"

    condition {
      tag_state  = "UNTAGGED"
      older_than = "7d"
    }
  }
}