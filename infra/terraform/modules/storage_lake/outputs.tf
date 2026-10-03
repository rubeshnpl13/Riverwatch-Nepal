output "bucket_name" {
  description = "RiverWatch data lake bucket name."
  value       = google_storage_bucket.this.name
}


output "bucket_uri" {
  description = "RiverWatch data lake gs:// URI."
  value       = "gs://${google_storage_bucket.this.name}"
}


output "managed_folders" {
  description = "Managed folder names keyed by logical path."

  value = {
    for key, folder in google_storage_managed_folder.this :
    key => folder.name
  }
}