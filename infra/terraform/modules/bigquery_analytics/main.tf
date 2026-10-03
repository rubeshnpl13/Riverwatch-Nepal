resource "google_bigquery_dataset" "analytics" {
  project     = var.project_id
  dataset_id  = var.dataset_id
  friendly_name = "RiverWatch analytics"
  description = "Analytics dataset for RiverWatch river station and observation data."
  location    = var.location
  labels      = var.labels

  # Never delete populated analytics data implicitly.
  delete_contents_on_destroy = false
}


resource "google_bigquery_connection" "lake" {
  project     = var.project_id
  location    = var.location
  connection_id = var.connection_id
  friendly_name = "RiverWatch lake connection"
  description = "Delegated BigQuery access to RiverWatch processed Cloud Storage data."

  cloud_resource {}
}