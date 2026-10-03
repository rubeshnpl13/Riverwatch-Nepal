resource "google_cloud_run_v2_service" "this" {
  count = var.enabled ? 1 : 0

  project  = var.project_id
  location = var.location
  name     = var.name

  deletion_protection = false

  # Pub/Sub in the same project is recognized
  # as internal Cloud Run traffic.
  ingress = "INGRESS_TRAFFIC_INTERNAL_ONLY"

  labels = var.labels

  template {
    service_account = var.service_account_email
    timeout         = var.timeout

    max_instance_request_concurrency = (
      var.max_instance_request_concurrency
    )

    scaling {
      min_instance_count = (
        var.min_instance_count
      )

      max_instance_count = (
        var.max_instance_count
      )
    }

    containers {
      image = var.image

      ports {
        container_port = var.container_port
      }

      dynamic "env" {
        for_each = var.environment_variables

        content {
          name  = env.key
          value = env.value
        }
      }
    }
  }
}