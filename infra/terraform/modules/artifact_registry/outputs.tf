output "repository_id" {
  description = "Artifact Registry repository ID."
  value       = google_artifact_registry_repository.this.repository_id
}


output "repository_name" {
  description = "Fully qualified Artifact Registry repository name."
  value       = google_artifact_registry_repository.this.name
}


output "docker_repository_uri" {
  description = "Docker repository URI prefix."
  value       = "${var.location}-docker.pkg.dev/${var.project_id}/${google_artifact_registry_repository.this.repository_id}"
}