from __future__ import annotations

import re
from collections.abc import (
    Iterable,
)
from datetime import (
    UTC,
    datetime,
)
from decimal import Decimal
from typing import (
    Any,
    Protocol,
    cast,
)

from google.api_core.exceptions import (
    GoogleAPIError,
)
from google.cloud import bigquery

from riverwatch.analytics.errors import (
    BigQueryAnalyticsError,
    InvalidAnalyticsQueryError,
)
from riverwatch.analytics.model import (
    BasinCurrentSummary,
    CurrentNetworkSummary,
    CurrentRiverStation,
    StationObservation,
)

PROJECT_ID_PATTERN = re.compile(
    r"^[a-z][a-z0-9-]*[a-z0-9]$"
)

DATASET_ID_PATTERN = re.compile(
    r"^[A-Za-z0-9_]+$"
)


class _BigQueryRow(
    Protocol,
):
    def __getitem__(
        self,
        key: str,
    ) -> Any:
        ...


class _BigQueryQueryJob(
    Protocol,
):
    def result(
        self,
    ) -> Iterable[_BigQueryRow]:
        ...


class _BigQueryClient(
    Protocol,
):
    def query(
        self,
        query: str,
        *,
        job_config: (
            bigquery.QueryJobConfig
            | None
        ) = None,
        location: str | None = None,
    ) -> _BigQueryQueryJob:
        ...


def _as_utc(
    value: datetime | None,
) -> datetime | None:
    if value is None:
        return None

    if (
        value.tzinfo is None
        or value.utcoffset() is None
    ):
        return value.replace(
            tzinfo=UTC
        )

    return value.astimezone(
        UTC
    )


def _query_timestamp(
    value: datetime,
) -> datetime:
    normalized = _as_utc(
        value
    )

    assert normalized is not None

    return normalized


def _required_station_id(
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


def _optional_string(
    value: object,
) -> str | None:
    if value is None:
        return None

    return str(
        value
    )


def _optional_float(
    value: object,
) -> float | None:
    if value is None:
        return None

    if isinstance(
        value,
        (
            int,
            float,
            str,
            Decimal,
        ),
    ):
        return float(
            value
        )

    raise RuntimeError(
        "Expected numeric BigQuery value"
    )


def _optional_datetime(
    value: object,
) -> datetime | None:
    if value is None:
        return None

    if not isinstance(
        value,
        datetime,
    ):
        raise RuntimeError(
            "Expected BigQuery TIMESTAMP "
            "value"
        )

    return _as_utc(
        value
    )


def _required_datetime(
    value: object,
    *,
    field_name: str,
) -> datetime:
    result = _optional_datetime(
        value
    )

    if result is None:
        raise RuntimeError(
            f"{field_name} returned null"
        )

    return result


def _current_station_from_row(
    row: _BigQueryRow,
) -> CurrentRiverStation:
    return CurrentRiverStation(
        station_id=_optional_string(
            row["station_id"]
        ),
        station_name=_optional_string(
            row["station_name"]
        ),
        basin_name=_optional_string(
            row["basin_name"]
        ),
        latitude=_optional_float(
            row["latitude"]
        ),
        longitude=_optional_float(
            row["longitude"]
        ),
        elevation_m=_optional_float(
            row["elevation_m"]
        ),
        source_record_id=_optional_string(
            row["source_record_id"]
        ),
        observed_at=_optional_datetime(
            row["observed_at"]
        ),
        water_level_m=_optional_float(
            row["water_level_m"]
        ),
        observation_ingested_at=(
            _optional_datetime(
                row[
                    "observation_ingested_at"
                ]
            )
        ),
        has_observation=bool(
            row["has_observation"]
        ),
        observation_age_hours=(
            _optional_float(
                row[
                    "observation_age_hours"
                ]
            )
        ),
    )


def _station_observation_from_row(
    row: _BigQueryRow,
) -> StationObservation:
    station_id = _optional_string(
        row["station_id"]
    )

    if station_id is None:
        raise RuntimeError(
            "station_observation_history "
            "returned a null station_id"
        )

    return StationObservation(
        source_record_id=(
            _optional_string(
                row["source_record_id"]
            )
        ),
        station_id=station_id,
        observed_at=(
            _required_datetime(
                row["observed_at"],
                field_name=(
                    "station_observation_history "
                    "observed_at"
                ),
            )
        ),
        water_level_m=_optional_float(
            row["water_level_m"]
        ),
        endpoint=str(
            row["endpoint"]
        ),
        run_id=str(
            row["run_id"]
        ),
        ingested_at=_optional_datetime(
            row["ingested_at"]
        ),
    )


def _basin_summary_from_row(
    row: _BigQueryRow,
) -> BasinCurrentSummary:
    return BasinCurrentSummary(
        basin_name=str(
            row["basin_name"]
        ),
        total_stations=int(
            row["total_stations"]
        ),
        stations_with_observation=int(
            row[
                "stations_with_observation"
            ]
        ),
        stations_without_observation=int(
            row[
                "stations_without_observation"
            ]
        ),
        fresh_observations=int(
            row["fresh_observations"]
        ),
        stale_observations=int(
            row["stale_observations"]
        ),
        future_observations=int(
            row["future_observations"]
        ),
        unassessable_observations=int(
            row[
                "unassessable_observations"
            ]
        ),
        observations_with_water_level=int(
            row[
                "observations_with_water_level"
            ]
        ),
        observations_without_water_level=int(
            row[
                "observations_without_water_level"
            ]
        ),
        coverage_ratio=float(
            row["coverage_ratio"]
        ),
        freshness_ratio=float(
            row["freshness_ratio"]
        ),
        oldest_observed_at=(
            _optional_datetime(
                row["oldest_observed_at"]
            )
        ),
        latest_observed_at=(
            _optional_datetime(
                row["latest_observed_at"]
            )
        ),
    )


def _network_summary_from_row(
    row: _BigQueryRow,
) -> CurrentNetworkSummary:
    return CurrentNetworkSummary(
        basins_represented=int(
            row["basins_represented"]
        ),
        total_stations=int(
            row["total_stations"]
        ),
        stations_with_observation=int(
            row[
                "stations_with_observation"
            ]
        ),
        stations_without_observation=int(
            row[
                "stations_without_observation"
            ]
        ),
        fresh_observations=int(
            row["fresh_observations"]
        ),
        stale_observations=int(
            row["stale_observations"]
        ),
        future_observations=int(
            row["future_observations"]
        ),
        unassessable_observations=int(
            row[
                "unassessable_observations"
            ]
        ),
        observations_with_water_level=int(
            row[
                "observations_with_water_level"
            ]
        ),
        observations_without_water_level=int(
            row[
                "observations_without_water_level"
            ]
        ),
        oldest_observed_at=(
            _optional_datetime(
                row["oldest_observed_at"]
            )
        ),
        latest_observed_at=(
            _optional_datetime(
                row["latest_observed_at"]
            )
        ),
        coverage_ratio=float(
            row["coverage_ratio"]
        ),
        freshness_ratio=float(
            row["freshness_ratio"]
        ),
    )


class BigQueryAnalyticsService:
    def __init__(
        self,
        *,
        project_id: str,
        dataset_id: str,
        location: str,
        client: (
            _BigQueryClient | None
        ) = None,
    ) -> None:
        self._project_id = (
            self._validate_project_id(
                project_id
            )
        )

        self._dataset_id = (
            self._validate_dataset_id(
                dataset_id
            )
        )

        self._location = (
            self._required_value(
                location,
                "location",
            )
        )

        if client is None:
            resolved_client = (
                bigquery.Client(
                    project=(
                        self._project_id
                    ),
                    location=(
                        self._location
                    ),
                )
            )

            self._client = cast(
                _BigQueryClient,
                resolved_client,
            )

        else:
            self._client = client



    def get_current_snapshot(
        self,
    ) -> tuple[
        CurrentRiverStation,
        ...,
    ]:
        rows = self._query(
            f"""
            SELECT
                station_id,
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

            FROM
                `{self._relation(
                    "current_river_snapshot"
                )}`

            ORDER BY
                station_name NULLS LAST,
                station_id NULLS LAST
            """
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
            _required_station_id(
                station_id
            )
        )

        job_config = (
            bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery
                    .ScalarQueryParameter(
                        "station_id",
                        "STRING",
                        station_id,
                    )
                ]
            )
        )

        rows = self._query(
            f"""
            SELECT
                station_id,
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

            FROM
                `{self._relation(
                    "current_river_snapshot"
                )}`

            WHERE
                station_id = @station_id

            LIMIT 1
            """,
            job_config=job_config,
        )

        if not rows:
            return None

        return _current_station_from_row(
            rows[0]
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
            _required_station_id(
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
            "station_id = @station_id",
        ]

        parameters = [
            bigquery.ScalarQueryParameter(
                "station_id",
                "STRING",
                station_id,
            )
        ]

        if start_at is not None:
            clauses.append(
                "observed_at >= @start_at"
            )

            parameters.append(
                bigquery
                .ScalarQueryParameter(
                    "start_at",
                    "TIMESTAMP",
                    _query_timestamp(
                        start_at
                    ),
                )
            )

        if end_at is not None:
            clauses.append(
                "observed_at < @end_at"
            )

            parameters.append(
                bigquery
                .ScalarQueryParameter(
                    "end_at",
                    "TIMESTAMP",
                    _query_timestamp(
                        end_at
                    ),
                )
            )

        where_clause = (
            " AND ".join(
                clauses
            )
        )

        job_config = (
            bigquery.QueryJobConfig(
                query_parameters=parameters
            )
        )

        rows = self._query(
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
                `{self._relation(
                    "station_observation_history"
                )}`

            WHERE
                {where_clause}

            ORDER BY
                observed_at ASC,
                source_record_id ASC
                    NULLS LAST
            """,
            job_config=job_config,
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
        rows = self._query(
            f"""
            SELECT
                basin_name,

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

            FROM
                `{self._relation(
                    "basin_current_summary"
                )}`

            ORDER BY
                total_stations DESC,
                basin_name
            """
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
        rows = self._query(
            f"""
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

            FROM
                `{self._relation(
                    "current_network_summary"
                )}`
            """
        )

        if not rows:
            raise RuntimeError(
                "current_network_summary "
                "returned no row"
            )

        return _network_summary_from_row(
            rows[0]
        )

    def _query(
        self,
        query: str,
        *,
        job_config: (
            bigquery.QueryJobConfig
            | None
        ) = None,
    ) -> tuple[
        _BigQueryRow,
        ...,
    ]:
        try:
            job = self._client.query(
                query,
                job_config=job_config,
                location=self._location,
            )

            return tuple(
                job.result()
            )

        except GoogleAPIError as exc:
            raise BigQueryAnalyticsError(
                "BigQuery analytics query "
                "failed"
            ) from exc

    def _relation(
        self,
        table_id: str,
    ) -> str:
        return (
            f"{self._project_id}."
            f"{self._dataset_id}."
            f"{table_id}"
        )

    @staticmethod
    def _required_value(
        value: str,
        name: str,
    ) -> str:
        normalized = value.strip()

        if not normalized:
            raise ValueError(
                f"{name} must not be blank"
            )

        return normalized

    @staticmethod
    def _validate_project_id(
        project_id: str,
    ) -> str:
        normalized = (
            project_id.strip()
        )

        if not PROJECT_ID_PATTERN.fullmatch(
            normalized
        ):
            raise ValueError(
                "project_id contains "
                "invalid characters"
            )

        return normalized

    @staticmethod
    def _validate_dataset_id(
        dataset_id: str,
    ) -> str:
        normalized = (
            dataset_id.strip()
        )

        if not DATASET_ID_PATTERN.fullmatch(
            normalized
        ):
            raise ValueError(
                "dataset_id contains "
                "invalid characters"
            )

        return normalized

    def close(
            self,
    ) -> None:
        close = getattr(
            self._client,
            "close",
            None,
        )

        if callable(
                close
        ):
            close()