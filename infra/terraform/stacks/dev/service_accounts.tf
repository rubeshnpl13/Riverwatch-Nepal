locals {
  runtime_service_accounts = {
    ingestion = {
      account_id   = "${local.name_prefix}-ingestion"
      display_name = "RiverWatch ${var.environment} ingestion"
      description  = "Runtime identity for RiverWatch BIPAD ingestion."
    }

    processing = {
      account_id   = "${local.name_prefix}-processing"
      display_name = "RiverWatch ${var.environment} processing"
      description  = "Runtime identity for RiverWatch Spark processing."
    }

    api = {
      account_id   = "${local.name_prefix}-api"
      display_name = "RiverWatch ${var.environment} API"
      description  = "Runtime identity for the RiverWatch FastAPI service."
    }

    event_invoker = {
      account_id   = "${local.name_prefix}-event-invoker"
      display_name = "RiverWatch ${var.environment} event invoker"
      description  = "Identity used by authenticated event delivery to invoke private RiverWatch services."
    }
  }
}


module "runtime_service_accounts" {
  source = "../../modules/service_accounts"

  project_id = var.project_id
  accounts   = local.runtime_service_accounts

  depends_on = [
    module.project_services,
  ]
}