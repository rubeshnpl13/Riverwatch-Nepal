output "emails" {
  description = "Service account email addresses."

  value = {
    for key, account in google_service_account.this :
    key => account.email
  }
}


output "members" {
  description = "IAM member strings for the service accounts."

  value = {
    for key, account in google_service_account.this :
    key => "serviceAccount:${account.email}"
  }
}


output "names" {
  description = "Fully qualified service account resource names."

  value = {
    for key, account in google_service_account.this :
    key => account.name
  }
}