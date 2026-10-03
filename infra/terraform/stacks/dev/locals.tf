locals {
  application = "riverwatch"

  name_prefix = (
    "${local.application}-${var.environment}"
  )

  common_labels = {
    application = local.application
    environment = var.environment
    managed_by  = "terraform"
  }
}