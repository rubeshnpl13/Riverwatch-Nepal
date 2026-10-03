variable "project_id" {
  description = "GCP project containing the RiverWatch data lake."
  type        = string

  validation {
    condition     = length(trimspace(var.project_id)) > 0
    error_message = "project_id must not be blank."
  }
}


variable "bucket_name" {
  description = "Globally unique Cloud Storage bucket name."
  type        = string

  validation {
    condition = (
      length(var.bucket_name) >= 3 &&
      length(var.bucket_name) <= 63
    )

    error_message = "bucket_name must be between 3 and 63 characters."
  }
}


variable "location" {
  description = "Cloud Storage bucket location."
  type        = string

  validation {
    condition     = length(trimspace(var.location)) > 0
    error_message = "location must not be blank."
  }
}


variable "labels" {
  description = "Labels applied to the RiverWatch lake bucket."
  type        = map(string)
  default     = {}
}


variable "managed_folders" {
  description = "Top-level managed folders in the RiverWatch data lake."
  type        = set(string)

  validation {
    condition = alltrue([
      for folder in var.managed_folders : (
        length(folder) > 1 && endswith(folder, "/")
      )
    ])

    error_message = "Each managed folder must be non-empty and end with '/'."
  }
}


variable "soft_delete_retention_seconds" {
  description = "Cloud Storage soft-delete retention. Use 0 to disable it."
  type        = number
  default     = 0

  validation {
    condition = (
      var.soft_delete_retention_seconds == 0 ||
      (
        var.soft_delete_retention_seconds >= 604800 &&
        var.soft_delete_retention_seconds <= 7776000
      )
    )

    error_message = "soft_delete_retention_seconds must be 0 or between 604800 and 7776000 seconds."
  }
}