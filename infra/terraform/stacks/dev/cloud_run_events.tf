locals {
  container_release_tag = "phase-11-8d-rc1"

  ingestion_event_image = (
    "${module.container_registry.docker_repository_uri}/ingestion:${local.container_release_tag}"
  )

  processing_event_image = (
    "${module.container_registry.docker_repository_uri}/processing:${local.container_release_tag}"
  )
  processing_completion_relay_image = (
    "${module.container_registry.docker_repository_uri}/processing-completion-relay:${local.container_release_tag}"
  )

  processing_spark_image = (
    "${module.container_registry.docker_repository_uri}/processing-spark:${local.container_release_tag}"
  )

  ingestion_completed_topic_path = (
    "projects/${var.project_id}/topics/${local.name_prefix}-ingestion-completed"
  )

  processing_completed_topic_path = (
    "projects/${var.project_id}/topics/${local.name_prefix}-processing-completed"
  )
}


module "ingestion_event_service" {
  source = "../../modules/cloud_run_service"

  project_id = var.project_id
  location   = var.region

  name                  = "${local.name_prefix}-ingestion"
  image                 = local.ingestion_event_image
  service_account_email = module.runtime_service_accounts.emails["ingestion"]

  labels = local.common_labels

  min_instance_count               = 0
  max_instance_count               = 1
  max_instance_request_concurrency = 1

  timeout = "300s"

  environment_variables = {
    RIVERWATCH_ENVIRONMENT = var.environment
    RIVERWATCH_LAKE_BUCKET = module.data_lake.bucket_name

    RIVERWATCH_INGESTION_COMPLETED_TOPIC = local.ingestion_completed_topic_path
  }

  enabled = var.enable_cloud_run_event_services

  depends_on = [
    module.project_services,
    module.container_registry,
    module.runtime_service_accounts,
  ]

  ingress              = "INGRESS_TRAFFIC_INTERNAL_ONLY"
  invoker_iam_disabled = false
}


module "processing_event_service" {
  source = "../../modules/cloud_run_service"

  project_id = var.project_id
  location   = var.region

  name                  = "${local.name_prefix}-processing"
  image                 = local.processing_event_image
  service_account_email = module.runtime_service_accounts.emails["processing_gateway"]

  labels = local.common_labels

  min_instance_count               = 0
  max_instance_count               = 1
  max_instance_request_concurrency = 1

  timeout = "300s"

  environment_variables = {
    RIVERWATCH_ENVIRONMENT = var.environment

    RIVERWATCH_DATAPROC_PROJECT_ID = var.project_id

    RIVERWATCH_DATAPROC_REGION = var.region

    RIVERWATCH_DATAPROC_RUNTIME_VERSION = var.dataproc_runtime_version

    RIVERWATCH_DATAPROC_WORKLOAD_SERVICE_ACCOUNT = module.runtime_service_accounts.emails["processing"]

    RIVERWATCH_DATAPROC_CONTAINER_IMAGE = local.processing_spark_image
    RIVERWATCH_LAKE_BUCKET = (
      module.data_lake.bucket_name
    )
  }

  enabled = var.enable_cloud_run_event_services

  depends_on = [
    module.project_services,
    module.container_registry,
    module.runtime_service_accounts,
  ]

  ingress              = "INGRESS_TRAFFIC_INTERNAL_ONLY"
  invoker_iam_disabled = false
}

module "processing_completion_relay_service" {
  source = "../../modules/cloud_run_service"

  project_id = var.project_id
  location   = var.region

  name = (
    "${local.name_prefix}-processing-completion-relay"
  )

  image = (
    local.processing_completion_relay_image
  )

  service_account_email = (
    module.runtime_service_accounts.emails[
      "processing_relay"
    ]
  )

  labels = local.common_labels

  min_instance_count               = 0
  max_instance_count               = 1
  max_instance_request_concurrency = 1

  timeout = "300s"

  environment_variables = {
    RIVERWATCH_LAKE_BUCKET = (
      module.data_lake.bucket_name
    )

    RIVERWATCH_PROCESSING_COMPLETED_TOPIC = (
      local.processing_completed_topic_path
    )
  }

  enabled = (
    var.enable_cloud_run_event_services
  )

  depends_on = [
    module.project_services,
    module.container_registry,
    module.runtime_service_accounts,
  ]

  ingress              = "INGRESS_TRAFFIC_INTERNAL_ONLY"
  invoker_iam_disabled = false
}


resource "google_cloud_run_v2_service_iam_member" "ingestion_event_invoker" {
  count = var.enable_cloud_run_event_services ? 1 : 0

  project  = var.project_id
  location = var.region
  name     = module.ingestion_event_service.name

  role = "roles/run.invoker"

  member = module.runtime_service_accounts.members["event_invoker"]
}


resource "google_cloud_run_v2_service_iam_member" "processing_event_invoker" {
  count = var.enable_cloud_run_event_services ? 1 : 0

  project  = var.project_id
  location = var.region
  name     = module.processing_event_service.name

  role = "roles/run.invoker"

  member = module.runtime_service_accounts.members["event_invoker"]
}
resource "google_cloud_run_v2_service_iam_member" "processing_completion_relay_invoker" {
  count = (
    var.enable_cloud_run_event_services
    ? 1
    : 0
  )

  project  = var.project_id
  location = var.region

  name = (
    module
    .processing_completion_relay_service
    .name
  )

  role = "roles/run.invoker"

  member = (
    module.runtime_service_accounts.members[
      "event_invoker"
    ]
  )
}