locals {
  # Must remain aligned with
  # DEFAULT_OBSERVATION_QUALITY_POLICY.current_max_age.
  analytics_freshness_hours = 24.0
}


resource "google_bigquery_table" "latest_current_run" {
  project    = var.project_id
  dataset_id = module.analytics.dataset_id
  table_id   = "latest_current_run"

  friendly_name = "RiverWatch latest current run"
  description   = "Latest processed river-stations run used by current RiverWatch analytics."
  labels        = local.common_labels

  view {
    use_legacy_sql = false

    query = <<-SQL
      SELECT
        run_id,
        MAX(ingested_at) AS ingested_at
      FROM
        `${var.project_id}.${module.analytics.dataset_id}.${google_bigquery_table.stations.table_id}`
      WHERE
        endpoint = 'river-stations'
      GROUP BY
        run_id
      ORDER BY
        ingested_at DESC,
        run_id DESC
      LIMIT 1
    SQL
  }
}


resource "google_bigquery_table" "current_stations" {
  project    = var.project_id
  dataset_id = module.analytics.dataset_id
  table_id   = "current_stations"

  friendly_name = "RiverWatch current stations"
  description   = "Stations belonging to the latest processed river-stations run."
  labels        = local.common_labels

  view {
    use_legacy_sql = false

    query = <<-SQL
      SELECT
        stations.*
      FROM
        `${var.project_id}.${module.analytics.dataset_id}.${google_bigquery_table.stations.table_id}` AS stations
      INNER JOIN
        `${var.project_id}.${module.analytics.dataset_id}.${google_bigquery_table.latest_current_run.table_id}` AS latest
      USING (run_id)
      WHERE
        stations.endpoint = 'river-stations'
    SQL
  }
}


resource "google_bigquery_table" "current_observations" {
  project    = var.project_id
  dataset_id = module.analytics.dataset_id
  table_id   = "current_observations"

  friendly_name = "RiverWatch current observations"
  description   = "Observations belonging to the latest processed river-stations run."
  labels        = local.common_labels

  view {
    use_legacy_sql = false

    query = <<-SQL
      SELECT
        observations.*
      FROM
        `${var.project_id}.${module.analytics.dataset_id}.${google_bigquery_table.observations.table_id}` AS observations
      INNER JOIN
        `${var.project_id}.${module.analytics.dataset_id}.${google_bigquery_table.latest_current_run.table_id}` AS latest
      USING (run_id)
      WHERE
        observations.endpoint = 'river-stations'
    SQL
  }
}


resource "google_bigquery_table" "historical_observations" {
  project    = var.project_id
  dataset_id = module.analytics.dataset_id
  table_id   = "historical_observations"

  friendly_name = "RiverWatch historical observations"
  description   = "Deduplicated historical river observations."
  labels        = local.common_labels

  view {
    use_legacy_sql = false

    query = <<-SQL
      SELECT
        *
      FROM
        `${var.project_id}.${module.analytics.dataset_id}.${google_bigquery_table.observations.table_id}`
      WHERE
        endpoint = 'river'
      QUALIFY
        source_record_id IS NULL
        OR TRIM(source_record_id) = ''
        OR ROW_NUMBER() OVER (
          PARTITION BY
            source_record_id
          ORDER BY
            ingested_at DESC,
            run_id DESC
        ) = 1
    SQL
  }
}


resource "google_bigquery_table" "station_observation_history" {
  project    = var.project_id
  dataset_id = module.analytics.dataset_id
  table_id   = "station_observation_history"

  friendly_name = "RiverWatch station observation history"
  description   = "Combined and deduplicated current and historical station observations."
  labels        = local.common_labels

  view {
    use_legacy_sql = false

    query = <<-SQL
      WITH combined AS (
        SELECT
          current_observations.*,
          1 AS _source_priority
        FROM
          `${var.project_id}.${module.analytics.dataset_id}.${google_bigquery_table.current_observations.table_id}` AS current_observations

        UNION ALL

        SELECT
          historical_observations.*,
          0 AS _source_priority
        FROM
          `${var.project_id}.${module.analytics.dataset_id}.${google_bigquery_table.historical_observations.table_id}` AS historical_observations
      )

      SELECT
        * EXCEPT (_source_priority)
      FROM
        combined
      WHERE
        station_id IS NOT NULL
        AND TRIM(station_id) != ''
        AND observed_at IS NOT NULL
      QUALIFY
        ROW_NUMBER() OVER (
          PARTITION BY
            station_id,
            observed_at
          ORDER BY
            ingested_at DESC NULLS LAST,
            _source_priority DESC,
            source_record_id DESC NULLS LAST
        ) = 1
    SQL
  }
}


resource "google_bigquery_table" "latest_observation_per_station" {
  project    = var.project_id
  dataset_id = module.analytics.dataset_id
  table_id   = "latest_observation_per_station"

  friendly_name = "RiverWatch latest observation per station"
  description   = "Latest known observation for each RiverWatch station."
  labels        = local.common_labels

  view {
    use_legacy_sql = false

    query = <<-SQL
      SELECT
        *
      FROM
        `${var.project_id}.${module.analytics.dataset_id}.${google_bigquery_table.station_observation_history.table_id}`
      QUALIFY
        ROW_NUMBER() OVER (
          PARTITION BY
            station_id
          ORDER BY
            observed_at DESC NULLS LAST,
            ingested_at DESC NULLS LAST,
            source_record_id DESC NULLS LAST
        ) = 1
    SQL
  }
}


resource "google_bigquery_table" "current_observation_per_station" {
  project    = var.project_id
  dataset_id = module.analytics.dataset_id
  table_id   = "current_observation_per_station"

  friendly_name = "RiverWatch current observation per station"
  description   = "Latest observation per station within the latest current ingestion run."
  labels        = local.common_labels

  view {
    use_legacy_sql = false

    query = <<-SQL
      SELECT
        *
      FROM
        `${var.project_id}.${module.analytics.dataset_id}.${google_bigquery_table.current_observations.table_id}`
      WHERE
        station_id IS NOT NULL
        AND TRIM(station_id) != ''
      QUALIFY
        ROW_NUMBER() OVER (
          PARTITION BY
            station_id
          ORDER BY
            observed_at DESC NULLS LAST,
            ingested_at DESC NULLS LAST,
            source_record_id DESC NULLS LAST
        ) = 1
    SQL
  }
}


resource "google_bigquery_table" "current_river_snapshot" {
  project    = var.project_id
  dataset_id = module.analytics.dataset_id
  table_id   = "current_river_snapshot"

  friendly_name = "RiverWatch current river snapshot"
  description   = "Current station network joined to each station's latest current observation."
  labels        = local.common_labels

  view {
    use_legacy_sql = false

    query = <<-SQL
      SELECT
        stations.*,

        observations.source_record_id,
        observations.observed_at,
        observations.water_level_m,

        observations.ingested_at
          AS observation_ingested_at,

        observations.observed_at IS NOT NULL
          AS has_observation,

        CASE
          WHEN
            observations.observed_at IS NOT NULL
            AND observations.ingested_at IS NOT NULL
          THEN
            TIMESTAMP_DIFF(
              observations.ingested_at,
              observations.observed_at,
              SECOND
            ) / 3600.0

          ELSE NULL
        END AS observation_age_hours

      FROM
        `${var.project_id}.${module.analytics.dataset_id}.${google_bigquery_table.current_stations.table_id}` AS stations

      LEFT JOIN
        `${var.project_id}.${module.analytics.dataset_id}.${google_bigquery_table.current_observation_per_station.table_id}` AS observations

      USING (station_id)
    SQL
  }
}


resource "google_bigquery_table" "basin_current_summary" {
  project    = var.project_id
  dataset_id = module.analytics.dataset_id
  table_id   = "basin_current_summary"

  friendly_name = "RiverWatch basin current summary"
  description   = "Current RiverWatch network health and observation availability summarized by basin."
  labels        = local.common_labels

  view {
    use_legacy_sql = false

    query = <<-SQL
      WITH classified AS (
        SELECT
          COALESCE(
            NULLIF(
              TRIM(basin_name),
              ''
            ),
            'Unknown'
          ) AS basin_name,

          has_observation,
          observed_at,
          water_level_m,
          observation_age_hours,

          CASE
            WHEN NOT has_observation
            THEN 'missing'

            WHEN observation_age_hours IS NULL
            THEN 'unassessable'

            WHEN observation_age_hours < 0
            THEN 'future'

            WHEN observation_age_hours
              <= ${local.analytics_freshness_hours}
            THEN 'fresh'

            ELSE 'stale'
          END AS freshness_status

        FROM
          `${var.project_id}.${module.analytics.dataset_id}.${google_bigquery_table.current_river_snapshot.table_id}`
      )

      SELECT
        basin_name,

        COUNT(*) AS total_stations,

        COUNTIF(
          has_observation
        ) AS stations_with_observation,

        COUNTIF(
          NOT has_observation
        ) AS stations_without_observation,

        COUNTIF(
          freshness_status = 'fresh'
        ) AS fresh_observations,

        COUNTIF(
          freshness_status = 'stale'
        ) AS stale_observations,

        COUNTIF(
          freshness_status = 'future'
        ) AS future_observations,

        COUNTIF(
          freshness_status = 'unassessable'
        ) AS unassessable_observations,

        COUNTIF(
          has_observation
          AND water_level_m IS NOT NULL
        ) AS observations_with_water_level,

        COUNTIF(
          has_observation
          AND water_level_m IS NULL
        ) AS observations_without_water_level,

        CAST(
          COUNTIF(
            has_observation
          )
          AS FLOAT64
        )
        / COUNT(*) AS coverage_ratio,

        CASE
          WHEN COUNTIF(
            has_observation
          ) = 0
          THEN 0.0

          ELSE
            CAST(
              COUNTIF(
                freshness_status = 'fresh'
              )
              AS FLOAT64
            )
            /
            COUNTIF(
              has_observation
            )
        END AS freshness_ratio,

        MIN(
          observed_at
        ) AS oldest_observed_at,

        MAX(
          observed_at
        ) AS latest_observed_at

      FROM
        classified

      GROUP BY
        basin_name
    SQL
  }
}


resource "google_bigquery_table" "current_network_summary" {
  project    = var.project_id
  dataset_id = module.analytics.dataset_id
  table_id   = "current_network_summary"

  friendly_name = "RiverWatch current network summary"
  description   = "Current RiverWatch network-wide observation availability and freshness summary."
  labels        = local.common_labels

  view {
    use_legacy_sql = false

    query = <<-SQL
      WITH totals AS (
        SELECT
          COUNT(*) AS basins_represented,

          COALESCE(
            SUM(total_stations),
            0
          ) AS total_stations,

          COALESCE(
            SUM(
              stations_with_observation
            ),
            0
          ) AS stations_with_observation,

          COALESCE(
            SUM(
              stations_without_observation
            ),
            0
          ) AS stations_without_observation,

          COALESCE(
            SUM(
              fresh_observations
            ),
            0
          ) AS fresh_observations,

          COALESCE(
            SUM(
              stale_observations
            ),
            0
          ) AS stale_observations,

          COALESCE(
            SUM(
              future_observations
            ),
            0
          ) AS future_observations,

          COALESCE(
            SUM(
              unassessable_observations
            ),
            0
          ) AS unassessable_observations,

          COALESCE(
            SUM(
              observations_with_water_level
            ),
            0
          ) AS observations_with_water_level,

          COALESCE(
            SUM(
              observations_without_water_level
            ),
            0
          ) AS observations_without_water_level,

          MIN(
            oldest_observed_at
          ) AS oldest_observed_at,

          MAX(
            latest_observed_at
          ) AS latest_observed_at

        FROM
          `${var.project_id}.${module.analytics.dataset_id}.${google_bigquery_table.basin_current_summary.table_id}`
      )

      SELECT
        totals.*,

        CASE
          WHEN total_stations = 0
          THEN 0.0

          ELSE
            CAST(
              stations_with_observation
              AS FLOAT64
            )
            / total_stations
        END AS coverage_ratio,

        CASE
          WHEN stations_with_observation = 0
          THEN 0.0

          ELSE
            CAST(
              fresh_observations
              AS FLOAT64
            )
            / stations_with_observation
        END AS freshness_ratio

      FROM
        totals
    SQL
  }
}