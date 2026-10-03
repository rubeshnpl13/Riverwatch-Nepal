module "scheduled_ingestion" {
  source = "../../modules/scheduled_ingestion"

  project_id = var.project_id
  region     = var.region

  name = (
    "${local.name_prefix}-ingestion"
  )

  topic_id = (
    module.event_bus.topic_ids[
      "ingestion-requested"
    ]
  )

  schedule = (
    var.ingestion_schedule
  )

  time_zone = (
    var.scheduler_time_zone
  )

  provider_name = "bipad"
  endpoint      = "river-stations"

  enabled = (
    var.enable_ingestion_schedule
  )

  depends_on = [
    module.project_services,
    module.event_bus,
  ]
}