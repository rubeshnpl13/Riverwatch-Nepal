output "job_name" {
  description = "Cloud Scheduler job name when enabled."

  value = try(
    google_cloud_scheduler_job.this[0].name,
    null,
  )
}


output "trigger_payload" {
  description = "Static scheduled-ingestion transport payload."
  value       = local.trigger_payload
}