output "enabled_services" {
  description = "Google Cloud APIs managed by this module."
  value = sort([
    for service in google_project_service.this :
    service.service
  ])
}