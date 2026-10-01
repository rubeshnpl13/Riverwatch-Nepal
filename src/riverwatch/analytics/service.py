from datetime import (
    UTC,
    datetime,
)
from threading import RLock
from typing import Any

from duckdb import DuckDBPyConnection

from riverwatch.analytics.errors import (
    InvalidAnalyticsQueryError,
)
from riverwatch.analytics.model import (
    BasinCurrentSummary,
    CurrentNetworkSummary,
    CurrentRiverStation,
    StationObservation,
)


def _as_utc(
    value: datetime | None,
) -> datetime | None:
    if value is None:
        return None

    if value.tzinfo is None:
        return value.replace(
            tzinfo=UTC
        )

    return value.astimezone(
        UTC
    )


def _duckdb_timestamp(
    value: datetime,
) -> datetime:
    if value.tzinfo is None:
        return value

    return (
        value
        .astimezone(
            UTC
        )
        .replace(
            tzinfo=None
        )
    )


def _require_station_id(
    station_id: str,
) -> str:
    normalized = (
        station_id.strip()
    )

    if not normalized:
        raise InvalidAnalyticsQueryError(
            "station_id must not be blank"
        )

    return normalized


def _current_station_from_row(
    row: tuple[Any, ...],
) -> CurrentRiverStation:
    return CurrentRiverStation(
        station_id=row[0],
        station_name=row[1],
        basin_name=row[2],
        latitude=(
            None
            if row[3] is None
            else float(
                row[3]
            )
        ),
        longitude=(
            None
            if row[4] is None
            else float(
                row[4]
            )
        ),
        elevation_m=(
            None
            if row[5] is None
            else float(
                row[5]
            )
        ),
        source_record_id=row[6],
        observed_at=_as_utc(
            row[7]
        ),
        water_level_m=(
            None
            if row[8] is None
            else float(
                row[8]
            )
        ),
        observation_ingested_at=(
            _as_utc(
                row[9]
            )
        ),
        has_observation=bool(
            row[10]
        ),
        observation_age_hours=(
            None
            if row[11] is None
            else float(
                row[11]
            )
        ),
    )


def _station_observation_from_row(
    row: tuple[Any, ...],
) -> StationObservation:
    observed_at = _as_utc(
        row[2]
    )

    if observed_at is None:
        raise RuntimeError(
            "station_observation_history "
            "returned a null observed_at"
        )

    return StationObservation(
        source_record_id=row[0],
        station_id=str(
            row[1]
        ),
        observed_at=observed_at,
        water_level_m=(
            None
            if row[3] is None
            else float(
                row[3]
            )
        ),
        endpoint=str(
            row[4]
        ),
        run_id=str(
            row[5]
        ),
        ingested_at=_as_utc(
            row[6]
        ),
    )


def _basin_summary_from_row(
    row: tuple[Any, ...],
) -> BasinCurrentSummary:
    return BasinCurrentSummary(
        basin_name=str(
            row[0]
        ),
        total_stations=int(
            row[1]
        ),
        stations_with_observation=int(
            row[2]
        ),
        stations_without_observation=int(
            row[3]
        ),
        fresh_observations=int(
            row[4]
        ),
        stale_observations=int(
            row[5]
        ),
        future_observations=int(
            row[6]
        ),
        unassessable_observations=int(
            row[7]
        ),
        observations_with_water_level=int(
            row[8]
        ),
        observations_without_water_level=int(
            row[9]
        ),
        coverage_ratio=float(
            row[10]
        ),
        freshness_ratio=float(
            row[11]
        ),
        oldest_observed_at=_as_utc(
            row[12]
        ),
        latest_observed_at=_as_utc(
            row[13]
        ),
    )


def _network_summary_from_row(
    row: tuple[Any, ...],
) -> CurrentNetworkSummary:
    return CurrentNetworkSummary(
        basins_represented=int(
            row[0]
        ),
        total_stations=int(
            row[1]
        ),
        stations_with_observation=int(
            row[2]
        ),
        stations_without_observation=int(
            row[3]
        ),
        fresh_observations=int(
            row[4]
        ),
        stale_observations=int(
            row[5]
        ),
        future_observations=int(
            row[6]
        ),
        unassessable_observations=int(
            row[7]
        ),
        observations_with_water_level=int(
            row[8]
        ),
        observations_without_water_level=int(
            row[9]
        ),
        oldest_observed_at=_as_utc(
            row[10]
        ),
        latest_observed_at=_as_utc(
            row[11]
        ),
        coverage_ratio=float(
            row[12]
        ),
        freshness_ratio=float(
            row[13]
        ),
    )


class AnalyticsService:
    def __init__(
            self,
            *,
            connection: DuckDBPyConnection,
    ) -> None:
        self._connection = connection
        self._connection_lock = RLock()

    def get_current_snapshot(
        self,
    ) -> tuple[
        CurrentRiverStation,
        ...,
    ]:

        with self._connection_lock:
            rows = (
                self._connection.execute(
                    """
                    SELECT station_id,
                           station_name,

                           NULLIF(
                                   TRIM(basin_name),
                                   ''
                           ) AS basin_name,

                           latitude,
                           longitude,
                           elevation_m,

                           source_record_id,
                           observed_at,
                           water_level_m,

                           observation_ingested_at,
                           has_observation,
                           observation_age_hours

                    FROM current_river_snapshot

                    ORDER BY station_name
                        NULLS LAST,
                             station_id
                        NULLS LAST
                    """
                )
                .fetchall()
            )

        return tuple(
            _current_station_from_row(
                row
            )
            for row in rows
        )

    def get_station(
        self,
        station_id: str,
    ) -> CurrentRiverStation | None:
        station_id = (
            _require_station_id(
                station_id
            )
        )

        with self._connection_lock:
            row = (
                self._connection.execute(
                    """
                    SELECT station_id,
                           station_name,

                           NULLIF(
                                   TRIM(basin_name),
                                   ''
                           ) AS basin_name,

                           latitude,
                           longitude,
                           elevation_m,

                           source_record_id,
                           observed_at,
                           water_level_m,

                           observation_ingested_at,
                           has_observation,
                           observation_age_hours

                    FROM current_river_snapshot

                    WHERE station_id = ? LIMIT 1
                    """,
                    [station_id],
                )
                .fetchone()
            )

        if row is None:
            return None

        return _current_station_from_row(
            row
        )

    def get_station_history(
        self,
        station_id: str,
        *,
        start_at: datetime | None = None,
        end_at: datetime | None = None,
    ) -> tuple[
        StationObservation,
        ...,
    ]:
        station_id = (
            _require_station_id(
                station_id
            )
        )

        if (
            start_at is not None
            and end_at is not None
            and start_at >= end_at
        ):
            raise InvalidAnalyticsQueryError(
                "start_at must be earlier "
                "than end_at"
            )

        clauses = [
            "station_id = ?",
        ]

        parameters: list[Any] = [
            station_id,
        ]

        if start_at is not None:
            clauses.append(
                "observed_at >= ?"
            )

            parameters.append(
                _duckdb_timestamp(
                    start_at
                )
            )

        if end_at is not None:
            clauses.append(
                "observed_at < ?"
            )

            parameters.append(
                _duckdb_timestamp(
                    end_at
                )
            )

        where_clause = (
            " AND ".join(
                clauses
            )
        )

        with self._connection_lock:
            rows = (
                self._connection.execute(
                    f"""
                                  SELECT
                                      source_record_id,
                                      station_id,
                                      observed_at,
                                      water_level_m,
                                      endpoint,
                                      run_id,
                                      ingested_at

                                  FROM
                                      station_observation_history

                                  WHERE
                                      {where_clause}

                                  ORDER BY
                                      observed_at ASC,
                                      source_record_id ASC
                                          NULLS LAST
                                  """,
                    parameters,
                )
                .fetchall()
            )
        return tuple(
            _station_observation_from_row(
                row
            )
            for row in rows
        )

    def get_basin_summaries(
        self,
    ) -> tuple[
        BasinCurrentSummary,
        ...,
    ]:

        with self._connection_lock:
            rows = (
                self._connection.execute(
                    """
                    SELECT basin_name,

                           total_stations,

                           stations_with_observation,
                           stations_without_observation,

                           fresh_observations,
                           stale_observations,
                           future_observations,
                           unassessable_observations,

                           observations_with_water_level,
                           observations_without_water_level,

                           coverage_ratio,
                           freshness_ratio,

                           oldest_observed_at,
                           latest_observed_at

                    FROM basin_current_summary

                    ORDER BY total_stations DESC,
                             basin_name
                    """
                )
                .fetchall()
            )
        return tuple(
            _basin_summary_from_row(
                row
            )
            for row in rows
        )

    def get_network_summary(
        self,
    ) -> CurrentNetworkSummary:

        with self._connection_lock:
            row = (
                self._connection.execute(
                     """
                SELECT
                    basins_represented,

                    total_stations,

                    stations_with_observation,
                    stations_without_observation,

                    fresh_observations,
                    stale_observations,
                    future_observations,
                    unassessable_observations,

                    observations_with_water_level,
                    observations_without_water_level,

                    oldest_observed_at,
                    latest_observed_at,

                    coverage_ratio,
                    freshness_ratio

                FROM current_network_summary
                """
                )
                .fetchone()
            )

        if row is None:
            raise RuntimeError(
                "current_network_summary "
                "returned no row"
            )

        return (
            _network_summary_from_row(
                row
            )
        )