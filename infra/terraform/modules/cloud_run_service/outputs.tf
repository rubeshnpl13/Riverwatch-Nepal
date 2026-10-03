output "name" {
  description = "Cloud Run service name when enabled."

  value = try(
    google_cloud_run_v2_service.this[0].name,
    null,
  )
}


output "uri" {
  description = "Cloud Run service URI when enabled."

  value = try(
    google_cloud_run_v2_service.this[0].uri,
    null,
  )
}


output "id" {
  description = "Cloud Run service resource ID when enabled."

  value = try(
    google_cloud_run_v2_service.this[0].id,
    null,
  )
}