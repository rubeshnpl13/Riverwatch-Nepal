from duckdb import DuckDBPyConnection

from riverwatch.quality.model import (
    DEFAULT_OBSERVATION_QUALITY_POLICY,
)

LATEST_CURRENT_RUN_VIEW = (
    "latest_current_run"
)

CURRENT_STATIONS_VIEW = (
    "current_stations"
)

CURRENT_OBSERVATIONS_VIEW = (
    "current_observations"
)

HISTORICAL_OBSERVATIONS_VIEW = (
    "historical_observations"
)

STATION_OBSERVATION_HISTORY_VIEW = (
    "station_observation_history"
)

LATEST_OBSERVATION_PER_STATION_VIEW = (
    "latest_observation_per_station"
)

CURRENT_OBSERVATION_PER_STATION_VIEW = (
    "current_observation_per_station"
)

CURRENT_RIVER_SNAPSHOT_VIEW = (
    "current_river_snapshot"
)
BASIN_CURRENT_SUMMARY_VIEW = (
    "basin_current_summary"
)

CURRENT_NETWORK_SUMMARY_VIEW = (
    "current_network_summary"
)


def register_analytics_views(
    *,
    connection: DuckDBPyConnection,
) -> None:
    connection.execute(
        """
        CREATE OR REPLACE VIEW
        latest_current_run AS

        SELECT
            run_id,
            MAX(ingested_at)
                AS ingested_at

        FROM stations

        WHERE
            endpoint = 'river-stations'

        GROUP BY
            run_id

        ORDER BY
            ingested_at DESC,
            run_id DESC

        LIMIT 1
        """
    )

    connection.execute(
        """
        CREATE OR REPLACE VIEW
        current_stations AS

        SELECT
            stations.*

        FROM stations

        INNER JOIN latest_current_run
            USING (run_id)

        WHERE
            stations.endpoint
                = 'river-stations'
        """
    )

    connection.execute(
        """
        CREATE OR REPLACE VIEW
        current_observations AS

        SELECT
            observations.*

        FROM observations

        INNER JOIN latest_current_run
            USING (run_id)

        WHERE
            observations.endpoint
                = 'river-stations'
        """
    )

    connection.execute(
        """
        CREATE OR REPLACE VIEW
        historical_observations AS

        SELECT
            *

        FROM observations

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
        """
    )

    connection.execute(
        """
        CREATE OR REPLACE VIEW
        station_observation_history AS

        WITH combined AS (
            SELECT
                *,
                1 AS _source_priority

            FROM current_observations

            UNION ALL

            SELECT
                *,
                0 AS _source_priority

            FROM historical_observations
        )

        SELECT
            * EXCLUDE (
                _source_priority
            )

        FROM combined

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
                    ingested_at DESC
                        NULLS LAST,
                    _source_priority DESC,
                    source_record_id DESC
                        NULLS LAST
            ) = 1
        """
    )

    connection.execute(
        """
        CREATE OR REPLACE VIEW
        latest_observation_per_station AS

        SELECT
            *

        FROM station_observation_history

        QUALIFY
            ROW_NUMBER() OVER (
                PARTITION BY
                    station_id

                ORDER BY
                    observed_at DESC
                        NULLS LAST,
                    ingested_at DESC
                        NULLS LAST,
                    source_record_id DESC
                        NULLS LAST
            ) = 1
        """
    )

    connection.execute(
        """
        CREATE OR REPLACE VIEW
        current_observation_per_station AS

        SELECT
            *

        FROM current_observations

        WHERE
            station_id IS NOT NULL
            AND TRIM(station_id) != ''

        QUALIFY
            ROW_NUMBER() OVER (
                PARTITION BY
                    station_id

                ORDER BY
                    observed_at DESC
                        NULLS LAST,
                    ingested_at DESC
                        NULLS LAST,
                    source_record_id DESC
                        NULLS LAST
            ) = 1
        """
    )

    connection.execute(
        """
        CREATE OR REPLACE VIEW
        current_river_snapshot AS

        SELECT
            stations.*,

            observations.source_record_id,

            observations.observed_at,

            observations.water_level_m,

            observations.ingested_at
                AS observation_ingested_at,

            observations.observed_at
                IS NOT NULL
                AS has_observation,

            CASE
                WHEN
                    observations.observed_at
                        IS NOT NULL
                    AND observations.ingested_at
                        IS NOT NULL
                THEN
                    DATE_DIFF(
                        'second',
                        observations.observed_at,
                        observations.ingested_at
                    )
                    / 3600.0

                ELSE NULL
            END
                AS observation_age_hours

        FROM current_stations AS stations

        LEFT JOIN
            current_observation_per_station
                AS observations

            USING (station_id)
        """
    )
    freshness_hours = (
        DEFAULT_OBSERVATION_QUALITY_POLICY
        .current_max_age
        .total_seconds()
        / 3600.0
    )

    connection.execute(
        f"""
        CREATE OR REPLACE VIEW
        basin_current_summary AS

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

                    WHEN observation_age_hours
                        IS NULL
                    THEN 'unassessable'

                    WHEN observation_age_hours < 0
                    THEN 'future'

                    WHEN observation_age_hours
                        <= {freshness_hours}
                    THEN 'fresh'

                    ELSE 'stale'
                END AS freshness_status

            FROM current_river_snapshot
        )

        SELECT
            basin_name,

            COUNT(*)
                AS total_stations,

            COUNT(*) FILTER (
                WHERE has_observation
            )
                AS stations_with_observation,

            COUNT(*) FILTER (
                WHERE NOT has_observation
            )
                AS stations_without_observation,

            COUNT(*) FILTER (
                WHERE freshness_status = 'fresh'
            )
                AS fresh_observations,

            COUNT(*) FILTER (
                WHERE freshness_status = 'stale'
            )
                AS stale_observations,

            COUNT(*) FILTER (
                WHERE freshness_status = 'future'
            )
                AS future_observations,

            COUNT(*) FILTER (
                WHERE freshness_status
                    = 'unassessable'
            )
                AS unassessable_observations,

            COUNT(*) FILTER (
                WHERE
                    has_observation
                    AND water_level_m IS NOT NULL
            )
                AS observations_with_water_level,

            COUNT(*) FILTER (
                WHERE
                    has_observation
                    AND water_level_m IS NULL
            )
                AS observations_without_water_level,

            (
                COUNT(*) FILTER (
                    WHERE has_observation
                )::DOUBLE
                / COUNT(*)
            )
                AS coverage_ratio,

            CASE
                WHEN
                    COUNT(*) FILTER (
                        WHERE has_observation
                    ) = 0
                THEN 0.0

                ELSE
                    COUNT(*) FILTER (
                        WHERE
                            freshness_status
                                = 'fresh'
                    )::DOUBLE
                    /
                    COUNT(*) FILTER (
                        WHERE has_observation
                    )
            END
                AS freshness_ratio,

            MIN(observed_at)
                AS oldest_observed_at,

            MAX(observed_at)
                AS latest_observed_at

        FROM classified

        GROUP BY
            basin_name
        """
    )

    connection.execute(
        """
        CREATE OR REPLACE VIEW
        current_network_summary AS

        WITH totals AS (
            SELECT
                COUNT(*)
                    AS basins_represented,

                COALESCE(
                    SUM(total_stations),
                    0
                )
                    AS total_stations,

                COALESCE(
                    SUM(
                        stations_with_observation
                    ),
                    0
                )
                    AS stations_with_observation,

                COALESCE(
                    SUM(
                        stations_without_observation
                    ),
                    0
                )
                    AS stations_without_observation,

                COALESCE(
                    SUM(fresh_observations),
                    0
                )
                    AS fresh_observations,

                COALESCE(
                    SUM(stale_observations),
                    0
                )
                    AS stale_observations,

                COALESCE(
                    SUM(future_observations),
                    0
                )
                    AS future_observations,

                COALESCE(
                    SUM(
                        unassessable_observations
                    ),
                    0
                )
                    AS unassessable_observations,

                COALESCE(
                    SUM(
                        observations_with_water_level
                    ),
                    0
                )
                    AS observations_with_water_level,

                COALESCE(
                    SUM(
                        observations_without_water_level
                    ),
                    0
                )
                    AS observations_without_water_level,

                MIN(oldest_observed_at)
                    AS oldest_observed_at,

                MAX(latest_observed_at)
                    AS latest_observed_at

            FROM basin_current_summary
        )

        SELECT
            *,

            CASE
                WHEN total_stations = 0
                THEN 0.0

                ELSE
                    stations_with_observation::DOUBLE
                    / total_stations
            END
                AS coverage_ratio,

            CASE
                WHEN stations_with_observation = 0
                THEN 0.0

                ELSE
                    fresh_observations::DOUBLE
                    / stations_with_observation
            END
                AS freshness_ratio

        FROM totals
        """
    )