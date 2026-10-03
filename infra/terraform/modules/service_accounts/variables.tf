variable "project_id" {
  description = "GCP project containing the service accounts."
  type        = string

  validation {
    condition     = length(trimspace(var.project_id)) > 0
    error_message = "project_id must not be blank."
  }
}

variable "accounts" {
  description = "RiverWatch user-managed service accounts."

  type = map(object({
    account_id   = string
    display_name = string
    description  = string
  }))

  validation {
    condition = alltrue([
      for account in values(var.accounts) : (
        length(account.account_id) >= 6 &&
        length(account.account_id) <= 30 &&
        can(regex("^[a-z]([-a-z0-9]*[a-z0-9])$", account.account_id))
      )
    ])

    error_message = "Each account_id must be 6-30 characters and use lowercase letters, digits, and hyphens."
  }
}