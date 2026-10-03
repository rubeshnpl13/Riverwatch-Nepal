resource "google_storage_bucket" "this" {
  name     = var.bucket_name
  project  = var.project_id
  location = var.location

  storage_class = "STANDARD"

  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"

  force_destroy = false

  labels = var.labels

  soft_delete_policy {
    retention_duration_seconds = (
      var.soft_delete_retention_seconds
    )
  }
}


resource "google_storage_managed_folder" "this" {
  for_each = var.managed_folders

  bucket = google_storage_bucket.this.name
  name   = each.value
}