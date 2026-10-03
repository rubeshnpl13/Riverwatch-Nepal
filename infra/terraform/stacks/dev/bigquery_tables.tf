locals {
  stations_schema_path     = "${path.module}/../../schemas/bigquery/stations.json"
  observations_schema_path = "${path.module}/../../schemas/bigquery/observations.json"
}


resource "google_bigquery_table" "stations_external" {
  project       = var.project_id
  dataset_id    = module.analytics.dataset_id
  table_id      = "stations_external"
  friendly_name = "RiverWatch stations BigLake source"
  description   = "BigLake external table over RiverWatch processed station Parquet files."
  schema        = file(local.stations_schema_path)
  labels        = local.common_labels

  external_data_configuration {
    autodetect    = false
    source_format = "PARQUET"
    connection_id = module.analytics.connection_name

    source_uris = [
      "gs://${module.data_lake.bucket_name}/processed/dataset=stations/*.parquet"
    ]
  }

  depends_on = [
    google_storage_bucket_iam_member.bigquery_connection_bucket_reader,
    google_storage_managed_folder_iam_member.bigquery_connection_processed_viewer,
  ]
}


resource "google_bigquery_table" "observations_external" {
  project       = var.project_id
  dataset_id    = module.analytics.dataset_id
  table_id      = "observations_external"
  friendly_name = "RiverWatch observations BigLake source"
  description   = "BigLake external table over RiverWatch processed observation Parquet files."
  schema        = file(local.observations_schema_path)
  labels        = local.common_labels

  external_data_configuration {
    autodetect    = false
    source_format = "PARQUET"
    connection_id = module.analytics.connection_name

    source_uris = [
      "gs://${module.data_lake.bucket_name}/processed/dataset=observations/*.parquet"
    ]
  }

  depends_on = [
    google_storage_bucket_iam_member.bigquery_connection_bucket_reader,
    google_storage_managed_folder_iam_member.bigquery_connection_processed_viewer,
  ]
}


resource "google_bigquery_table" "stations" {
  project       = var.project_id
  dataset_id    = module.analytics.dataset_id
  table_id      = "stations"
  friendly_name = "RiverWatch stations"
  description   = "Canonical RiverWatch station surface including lake path metadata."
  labels        = local.common_labels

  view {
    use_legacy_sql = false

    query = <<-SQL
      SELECT
        source.*,
        REGEXP_EXTRACT(
          source._FILE_NAME,
          r'/endpoint=([^/]+)/'
        ) AS endpoint,
        SAFE_CAST(
          REGEXP_EXTRACT(
            source._FILE_NAME,
            r'/year=([0-9]{4})/'
          ) AS INT64
        ) AS year,
        SAFE_CAST(
          REGEXP_EXTRACT(
            source._FILE_NAME,
            r'/month=([0-9]{2})/'
          ) AS INT64
        ) AS month,
        SAFE_CAST(
          REGEXP_EXTRACT(
            source._FILE_NAME,
            r'/day=([0-9]{2})/'
          ) AS INT64
        ) AS day,
        SAFE_CAST(
          REGEXP_EXTRACT(
            source._FILE_NAME,
            r'/hour=([0-9]{2})/'
          ) AS INT64
        ) AS hour
      FROM
        `${var.project_id}.${module.analytics.dataset_id}.${google_bigquery_table.stations_external.table_id}` AS source
    SQL
  }
}


resource "google_bigquery_table" "observations" {
  project       = var.project_id
  dataset_id    = module.analytics.dataset_id
  table_id      = "observations"
  friendly_name = "RiverWatch observations"
  description   = "Canonical RiverWatch observation surface including lake path metadata."
  labels        = local.common_labels

  view {
    use_legacy_sql = false

    query = <<-SQL
      SELECT
        source.*,
        REGEXP_EXTRACT(
          source._FILE_NAME,
          r'/endpoint=([^/]+)/'
        ) AS endpoint,
        SAFE_CAST(
          REGEXP_EXTRACT(
            source._FILE_NAME,
            r'/year=([0-9]{4})/'
          ) AS INT64
        ) AS year,
        SAFE_CAST(
          REGEXP_EXTRACT(
            source._FILE_NAME,
            r'/month=([0-9]{2})/'
          ) AS INT64
        ) AS month,
        SAFE_CAST(
          REGEXP_EXTRACT(
            source._FILE_NAME,
            r'/day=([0-9]{2})/'
          ) AS INT64
        ) AS day,
        SAFE_CAST(
          REGEXP_EXTRACT(
            source._FILE_NAME,
            r'/hour=([0-9]{2})/'
          ) AS INT64
        ) AS hour
      FROM
        `${var.project_id}.${module.analytics.dataset_id}.${google_bigquery_table.observations_external.table_id}` AS source
    SQL
  }
}