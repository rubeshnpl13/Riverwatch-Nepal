module "container_registry" {
  source = "../../modules/artifact_registry"

  project_id = var.project_id
  location   = var.region

  repository_id = (
    "${local.name_prefix}-containers"
  )

  labels = local.common_labels

  depends_on = [
    module.project_services,
  ]
}