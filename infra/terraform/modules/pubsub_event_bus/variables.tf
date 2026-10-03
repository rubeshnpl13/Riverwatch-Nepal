variable "project_id" {
  description = "GCP project containing RiverWatch Pub/Sub resources."
  type        = string

  validation {
    condition     = length(trimspace(var.project_id)) > 0
    error_message = "project_id must not be blank."
  }
}


variable "name_prefix" {
  description = "Common RiverWatch resource name prefix."
  type        = string

  validation {
    condition     = length(trimspace(var.name_prefix)) > 0
    error_message = "name_prefix must not be blank."
  }
}


variable "labels" {
  description = "Labels applied to Pub/Sub resources."
  type        = map(string)
  default     = {}
}


variable "message_retention_duration" {
  description = "Subscription message retention duration."
  type        = string
  default     = "604800s"
}


variable "ack_deadline_seconds" {
  description = "Acknowledgement deadline for worker subscriptions."
  type        = number
  default     = 60

  validation {
    condition = (
      var.ack_deadline_seconds >= 10 &&
      var.ack_deadline_seconds <= 600
    )

    error_message = "ack_deadline_seconds must be between 10 and 600."
  }
}


variable "minimum_backoff" {
  description = "Minimum Pub/Sub redelivery backoff."
  type        = string
  default     = "10s"
}


variable "maximum_backoff" {
  description = "Maximum Pub/Sub redelivery backoff."
  type        = string
  default     = "300s"
}


variable "max_delivery_attempts" {
  description = "Maximum Pub/Sub delivery attempts before dead lettering."
  type        = number
  default     = 5

  validation {
    condition = (
      var.max_delivery_attempts >= 5 &&
      var.max_delivery_attempts <= 100
    )

    error_message = "max_delivery_attempts must be between 5 and 100."
  }
}