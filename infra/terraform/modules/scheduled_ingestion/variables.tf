variable "project_id" {
  description = "GCP project containing the scheduler job."
  type        = string

  validation {
    condition     = length(trimspace(var.project_id)) > 0
    error_message = "project_id must not be blank."
  }
}


variable "region" {
  description = "Cloud Scheduler job region."
  type        = string

  validation {
    condition     = length(trimspace(var.region)) > 0
    error_message = "region must not be blank."
  }
}


variable "name" {
  description = "Cloud Scheduler job name."
  type        = string

  validation {
    condition     = length(trimspace(var.name)) > 0
    error_message = "name must not be blank."
  }
}


variable "topic_id" {
  description = "Fully qualified ingestion-requested Pub/Sub topic ID."
  type        = string

  validation {
    condition     = length(trimspace(var.topic_id)) > 0
    error_message = "topic_id must not be blank."
  }
}


variable "schedule" {
  description = "Cron schedule for ingestion."
  type        = string

  validation {
    condition     = length(trimspace(var.schedule)) > 0
    error_message = "schedule must not be blank."
  }
}


variable "time_zone" {
  description = "Cloud Scheduler time zone."
  type        = string
  default     = "Etc/UTC"
}


variable "provider_name" {
  description = "RiverWatch ingestion provider."
  type        = string
}


variable "endpoint" {
  description = "RiverWatch ingestion endpoint."
  type        = string
}


variable "enabled" {
  description = "Whether to create the scheduler job."
  type        = bool
  default     = false
}