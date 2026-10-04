locals {
  dataproc_resource_manager_node_service_agent = (
    "service-${data.google_project.current.number}@gcp-sa-dataprocrmnode.iam.gserviceaccount.com"
  )

  dataproc_resource_manager_node_service_agent_member = (
    "serviceAccount:${local.dataproc_resource_manager_node_service_agent}"
  )
}


resource "google_project_iam_member" "processing_gateway_dataproc_submitter" {
  count = (
    var.enable_cloud_run_event_services
    ? 1
    : 0
  )

  project = var.project_id

  role = (
    "roles/dataproc.serverlessEditor"
  )

  member = (
    module.runtime_service_accounts.members[
      "processing_gateway"
    ]
  )
}


resource "google_service_account_iam_member" "processing_gateway_uses_processing_runtime" {
  count = (
    var.enable_cloud_run_event_services
    ? 1
    : 0
  )

  service_account_id = (
    module.runtime_service_accounts.names[
      "processing"
    ]
  )

  role = (
    "roles/iam.serviceAccountUser"
  )

  member = (
    module.runtime_service_accounts.members[
      "processing_gateway"
    ]
  )
}


resource "google_project_iam_member" "processing_runtime_serverless_node" {
  count = (
    var.enable_cloud_run_event_services
    ? 1
    : 0
  )

  project = var.project_id

  role = (
    "roles/dataproc.serverlessNode"
  )

  member = (
    module.runtime_service_accounts.members[
      "processing"
    ]
  )
}


resource "google_artifact_registry_repository_iam_member" "dataproc_container_reader" {
  count = (
    var.enable_cloud_run_event_services
    ? 1
    : 0
  )

  project    = var.project_id
  location   = var.region
  repository = module.container_registry.repository_id

  role = (
    "roles/artifactregistry.reader"
  )

  member = (
    local.dataproc_resource_manager_node_service_agent_member
  )
}