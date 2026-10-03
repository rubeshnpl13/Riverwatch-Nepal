variable "project_id" {
  description = "GCP project containing the Cloud Run service."
  type        = string

  validation {
    condition     = length(trimspace(var.project_id)) > 0
    error_message = "project_id must not be blank."
  }
}


variable "location" {
  description = "Cloud Run service location."
  type        = string

  validation {
    condition     = length(trimspace(var.location)) > 0
    error_message = "location must not be blank."
  }
}


variable "name" {
  description = "Cloud Run service name."
  type        = string

  validation {
    condition     = length(trimspace(var.name)) > 0
    error_message = "name must not be blank."
  }
}


variable "image" {
  description = "Container image deployed by Cloud Run."
  type        = string

  validation {
    condition     = length(trimspace(var.image)) > 0
    error_message = "image must not be blank."
  }
}


variable "service_account_email" {
  description = "Runtime service account used by the Cloud Run revision."
  type        = string

  validation {
    condition     = length(trimspace(var.service_account_email)) > 0
    error_message = "service_account_email must not be blank."
  }
}


variable "labels" {
  description = "Labels applied to the Cloud Run service."
  type        = map(string)
  default     = {}
}


variable "container_port" {
  description = "Container HTTP port."
  type        = number
  default     = 8080
}


variable "timeout" {
  description = "Maximum request duration."
  type        = string
  default     = "300s"
}


variable "min_instance_count" {
  description = "Minimum Cloud Run instances."
  type        = number
  default     = 0
}


variable "max_instance_count" {
  description = "Maximum Cloud Run instances."
  type        = number
  default     = 1
}


variable "max_instance_request_concurrency" {
  description = "Maximum concurrent requests per instance."
  type        = number
  default     = 1
}


variable "environment_variables" {
  description = "Environment variables passed to the container."
  type        = map(string)
  default     = {}
}


variable "enabled" {
  description = "Whether the Cloud Run service is created."
  type        = bool
  default     = false
}