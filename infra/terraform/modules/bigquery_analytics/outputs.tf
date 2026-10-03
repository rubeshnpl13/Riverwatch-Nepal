output "dataset_id" {
  description = "RiverWatch analytics BigQuery dataset ID."
  value       = google_bigquery_dataset.analytics.dataset_id
}


output "dataset_resource_id" {
  description = "Fully qualified BigQuery dataset resource ID."
  value       = google_bigquery_dataset.analytics.id
}


output "connection_id" {
  description = "RiverWatch BigQuery Cloud Resource connection ID."
  value       = google_bigquery_connection.lake.connection_id
}


output "connection_name" {
  description = "Fully qualified BigQuery connection resource name."
  value       = google_bigquery_connection.lake.name
}


output "connection_service_account_id" {
  description = "Service account used by the BigQuery lake connection."

  value = (
    google_bigquery_connection
    .lake
    .cloud_resource[0]
    .service_account_id
  )
}