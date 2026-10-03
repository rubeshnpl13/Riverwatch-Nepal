variable "project_id" {
  description = "GCP project in which APIs are enabled."
  type        = string

  validation {
    condition     = length(trimspace(var.project_id)) > 0
    error_message = "project_id must not be blank."
  }
}


variable "services" {
  description = "Google Cloud APIs to enable."
  type        = set(string)

  validation {
    condition = alltrue([
      for service in var.services : (
        length(trimspace(service)) > 0
      )
    ])

    error_message = "Service names must not be blank."
  }
}