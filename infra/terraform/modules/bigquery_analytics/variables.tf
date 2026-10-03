variable "project_id" {
  description = "GCP project containing RiverWatch analytics resources."
  type        = string

  validation {
    condition     = length(trimspace(var.project_id)) > 0
    error_message = "project_id must not be blank."
  }
}


variable "dataset_id" {
  description = "BigQuery dataset ID for RiverWatch analytics."
  type        = string

  validation {
    condition = can(
      regex(
        "^[A-Za-z0-9_]+$",
        var.dataset_id,
      )
    )

    error_message = "dataset_id may contain only letters, numbers, and underscores."
  }
}


variable "location" {
  description = "BigQuery dataset and connection location."
  type        = string

  validation {
    condition     = length(trimspace(var.location)) > 0
    error_message = "location must not be blank."
  }
}


variable "connection_id" {
  description = "BigQuery Cloud Resource connection ID."
  type        = string

  validation {
    condition     = length(trimspace(var.connection_id)) > 0
    error_message = "connection_id must not be blank."
  }
}


variable "labels" {
  description = "Labels applied to RiverWatch analytics resources."
  type        = map(string)
  default     = {}
}