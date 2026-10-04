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

variable "enable_ingestion_schedule" {
  description = "Whether the recurring development ingestion schedule is enabled."
  type        = bool
  default     = false
}


variable "ingestion_schedule" {
  description = "Cloud Scheduler cron expression for RiverWatch ingestion."
  type        = string
  default     = "5 * * * *"

  validation {
    condition = length(trimspace(var.ingestion_schedule)) > 0

    error_message = "ingestion_schedule must not be blank."
  }
}


variable "scheduler_time_zone" {
  description = "Cloud Scheduler time zone."
  type        = string
  default     = "Etc/UTC"

  validation {
    condition = length(trimspace(var.scheduler_time_zone)) > 0

    error_message = "scheduler_time_zone must not be blank."
  }
}

variable "enable_cloud_run_event_services" {
  description = "Whether the ingestion and processing event services are deployed to Cloud Run."
  type        = bool
  default     = false
}

variable "dataproc_runtime_version" {
  description = "Managed Service for Apache Spark runtime version."

  type    = string
  default = "3.0"

  validation {
    condition = (
      length(
        trimspace(
          var.dataproc_runtime_version
        )
      ) > 0
    )

    error_message = "dataproc_runtime_version must not be blank."
  }
}
variable "enable_api_service" {
  description = "Whether the RiverWatch FastAPI service is deployed to Cloud Run."

  type    = bool
  default = false
}

variable "api_cors_origins" {
  description = "Browser origins allowed by the RiverWatch FastAPI CORS policy."

  type = list(string)

  default = [
    "http://127.0.0.1:5173",
    "http://localhost:5173",
  ]

  validation {
    condition = (
      length(var.api_cors_origins) > 0 &&
      alltrue([
        for origin in var.api_cors_origins :
        length(trimspace(origin)) > 0
      ])
    )

    error_message = "api_cors_origins must contain at least one non-blank origin."
  }
}
