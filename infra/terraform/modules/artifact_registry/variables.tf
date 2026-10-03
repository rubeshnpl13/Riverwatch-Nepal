variable "project_id" {
  description = "GCP project containing the Artifact Registry repository."
  type        = string

  validation {
    condition     = length(trimspace(var.project_id)) > 0
    error_message = "project_id must not be blank."
  }
}


variable "location" {
  description = "Artifact Registry repository location."
  type        = string

  validation {
    condition     = length(trimspace(var.location)) > 0
    error_message = "location must not be blank."
  }
}


variable "repository_id" {
  description = "Artifact Registry Docker repository ID."
  type        = string

  validation {
    condition     = length(trimspace(var.repository_id)) > 0
    error_message = "repository_id must not be blank."
  }
}


variable "labels" {
  description = "Labels applied to the repository."
  type        = map(string)
  default     = {}
}