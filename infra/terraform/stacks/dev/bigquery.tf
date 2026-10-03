locals {
  analytics_dataset_id = (
    "${replace(local.name_prefix, "-", "_")}_analytics"
  )

  analytics_connection_id = (
    "${local.name_prefix}-lake"
  )
}


module "analytics" {
  source = "../../modules/bigquery_analytics"

  project_id = var.project_id

  dataset_id = (
    local.analytics_dataset_id
  )

  location = var.region

  connection_id = (
    local.analytics_connection_id
  )

  labels = local.common_labels

  depends_on = [
    module.project_services,
    module.data_lake,
  ]
}

resource "google_storage_bucket_iam_member" "bigquery_connection_bucket_reader" {
  bucket = module.data_lake.bucket_name

  role = "roles/storage.legacyBucketReader"

  member = (
    "serviceAccount:${module.analytics.connection_service_account_id}"
  )
}


resource "google_storage_managed_folder_iam_member" "bigquery_connection_processed_viewer" {
  bucket = module.data_lake.bucket_name

  managed_folder = (
    module.data_lake.managed_folders[
      "processed/"
    ]
  )

  role = "roles/storage.objectViewer"

  member = (
    "serviceAccount:${module.analytics.connection_service_account_id}"
  )
}

resource "google_project_iam_member" "api_bigquery_job_user" {
  project = var.project_id

  role = "roles/bigquery.jobUser"

  member = (
    module.runtime_service_accounts.members[
      "api"
    ]
  )
}


resource "google_bigquery_dataset_iam_member" "api_analytics_viewer" {
  project = var.project_id

  dataset_id = (
    module.analytics.dataset_id
  )

  role = "roles/bigquery.dataViewer"

  member = (
    module.runtime_service_accounts.members[
      "api"
    ]
  )
}