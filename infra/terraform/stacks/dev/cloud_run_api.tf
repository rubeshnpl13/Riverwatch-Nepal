locals {
  api_image = "${module.container_registry.docker_repository_uri}/api:phase-11-placeholder"

}



module "api_service" {
  source = "../../modules/cloud_run_service"

  project_id = var.project_id
  location   = var.region

  name = (
    "${local.name_prefix}-api"
  )

  image = (
    local.api_image
  )

  service_account_email = (
    module.runtime_service_accounts.emails[
      "api"
    ]
  )

  labels = local.common_labels

  # Dashboard/browser clients must be able
  # to reach the API over public HTTPS.
  ingress = "INGRESS_TRAFFIC_ALL"

  # This is the public read-only API.
  # Event services keep IAM invocation enabled.
  invoker_iam_disabled = true

  min_instance_count = 0
  max_instance_count = 1

  max_instance_request_concurrency = 20

  timeout = "60s"

  environment_variables = {
    RIVERWATCH_ENVIRONMENT = (
      var.environment
    )

    RIVERWATCH_ANALYTICS_BACKEND = (
      "bigquery"
    )

    RIVERWATCH_BIGQUERY_PROJECT_ID = (
      var.project_id
    )

    RIVERWATCH_BIGQUERY_DATASET_ID = (
      module.analytics.dataset_id
    )

    RIVERWATCH_BIGQUERY_LOCATION = (
      var.region
    )

    RIVERWATCH_CORS_ORIGINS = (
      join(
        ",",
        var.api_cors_origins,
      )
    )
  }

  enabled = (
    var.enable_api_service
  )

  depends_on = [
    module.project_services,
    module.container_registry,
    module.analytics,
    module.runtime_service_accounts,
  ]
}