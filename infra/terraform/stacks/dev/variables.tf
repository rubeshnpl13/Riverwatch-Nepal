variable "project_id" {
  description = "GCP project ID for RiverWatch."
  type        = string

  validation {
    condition = (
      length(
        trimspace(
          var.project_id
        )
      ) > 0
    )

    error_message = "project_id must not be blank."
  }
}


variable "region" {
  description = "Primary GCP region for RiverWatch."
  type        = string
  default     = "asia-south1"

  validation {
    condition = (
      length(
        trimspace(
          var.region
        )
      ) > 0
    )

    error_message = "region must not be blank."
  }
}


variable "environment" {
  description = "RiverWatch deployment environment."
  type        = string
  default     = "dev"

  validation {
    condition = contains(
      [
        "dev",
        "prod",
      ],
      var.environment,
    )

    error_message = "environment must be either 'dev' or 'prod'."
  }
}