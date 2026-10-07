locals {
  lake_bucket_name = (
    "${var.project_id}-${local.name_prefix}-lake"
  )

  lake_managed_folders = toset([
    ".staging/",
    "manifests/",
    "processed/",
    "quality/",
    "quarantine/",
    "raw/",
  ])
}


module "data_lake" {
  source = "../../modules/storage_lake"

  project_id  = var.project_id
  bucket_name = local.lake_bucket_name
  location    = upper(var.region)

  labels = local.common_labels

  managed_folders = (
    local.lake_managed_folders
  )

  # Development data is reproducible from upstream
  # sources, so avoid paying for soft-deleted copies.
  soft_delete_retention_seconds = 0

  depends_on = [
    module.project_services,
  ]
}

locals {
  ingestion_lake_access = {
    raw_creator = {
      folder = "raw/"
      role   = "roles/storage.objectCreator"
    }

    raw_viewer = {
      folder = "raw/"
      role   = "roles/storage.objectViewer"
    }

    quarantine_creator = {
      folder = "quarantine/"
      role   = "roles/storage.objectCreator"
    }
    quarantine_viewer = {
      folder = "quarantine/"
      role   = "roles/storage.objectViewer"
    }

    manifests_creator = {
      folder = "manifests/"
      role   = "roles/storage.objectCreator"
    }

    manifests_viewer = {
      folder = "manifests/"
      role   = "roles/storage.objectViewer"
    }
  }
}


resource "google_storage_managed_folder_iam_member" "ingestion" {
  for_each = local.ingestion_lake_access

  bucket = module.data_lake.bucket_name

  managed_folder = (
    module.data_lake.managed_folders[
      each.value.folder
    ]
  )

  role = each.value.role

  member = (
    module.runtime_service_accounts.members[
      "ingestion"
    ]
  )
}

locals {
  processing_lake_access = {
    raw_viewer = {
      folder = "raw/"
      role   = "roles/storage.objectViewer"
    }

    manifests_viewer = {
      folder = "manifests/"
      role   = "roles/storage.objectViewer"
    }

    staging_user = {
      folder = ".staging/"
      role   = "roles/storage.objectUser"
    }

    processed_user = {
      folder = "processed/"
      role   = "roles/storage.objectUser"
    }

    quality_creator = {
      folder = "quality/"
      role   = "roles/storage.objectCreator"
    }

    quality_viewer = {
      folder = "quality/"
      role   = "roles/storage.objectViewer"
    }
  }
}


resource "google_storage_managed_folder_iam_member" "processing" {
  for_each = local.processing_lake_access

  bucket = module.data_lake.bucket_name

  managed_folder = (
    module.data_lake.managed_folders[
      each.value.folder
    ]
  )

  role = each.value.role

  member = (
    module.runtime_service_accounts.members[
      "processing"
    ]
  )
}

resource "google_storage_notification" "processing_completion_receipts" {
  count = (
    var.enable_cloud_run_event_services
    ? 1
    : 0
  )

  bucket = (
    module.data_lake.bucket_name
  )

  payload_format = "NONE"

  topic = (
    google_pubsub_topic
    .processing_completion_receipt_notifications
    .id
  )

  event_types = [
    "OBJECT_FINALIZE",
  ]

  depends_on = [
    google_pubsub_topic_iam_member.processing_completion_receipt_gcs_publisher,
  ]
}

resource "google_storage_managed_folder_iam_member" "processing_completion_relay_quality_viewer" {
  bucket = (
    module.data_lake.bucket_name
  )

  managed_folder = (
    module.data_lake.managed_folders[
      "quality/"
    ]
  )

  role = "roles/storage.objectViewer"

  member = (
    module.runtime_service_accounts.members[
      "processing_relay"
    ]
  )
}