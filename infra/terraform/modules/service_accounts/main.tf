resource "google_service_account" "this" {
  for_each = var.accounts

  project = var.project_id

  account_id   = each.value.account_id
  display_name = each.value.display_name
  description  = each.value.description
}