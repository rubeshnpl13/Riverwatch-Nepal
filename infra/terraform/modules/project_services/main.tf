resource "google_project_service" "this" {
  for_each = var.services

  project = var.project_id
  service = each.value

  # RiverWatch does not manage the GCP project itself.
  # Destroying this Terraform stack therefore should not
  # disable shared project APIs.
  disable_on_destroy = false
}