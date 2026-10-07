from __future__ import annotations

from collections.abc import (
    Iterable,
)
from datetime import (
    UTC,
    datetime,
)

import pytest
from google.api_core.exceptions import (
    ServiceUnavailable,
)
from google.cloud import bigquery

from riverwatch.analytics.bigquery import (
    BigQueryAnalyticsService,
)
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

OBSERVED_AT = datetime(
    2026,
    10,
    5,
    1,
    0,
    tzinfo=UTC,
)

INGESTED_AT = datetime(
    2026,
    10,
    5,
    1,
    5,
    tzinfo=UTC,
)


class FakeQueryJob:
    def __init__(
        self,
        *,
        rows: list[
            dict[str, object]
        ],
        error: Exception | None = None,
    ) -> None:
        self._rows = rows
        self._error = error

    def result(
        self,
    ) -> Iterable[
        dict[str, object]
    ]:
        if self._error is not None:
            raise self._error

        return self._rows


class FakeBigQueryClient:
    def __init__(
        self,
        *,
        rows: list[
            dict[str, object]
        ] | None = None,
        query_error: Exception | None = None,
        result_error: Exception | None = None,
    ) -> None:
        self.rows = (
            []
            if rows is None
            else rows
        )

        self.query_error = query_error
        self.result_error = (
            result_error
        )

        self.calls: list[
            tuple[
                str,
                bigquery.QueryJobConfig
                | None,
                str | None,
            ]
        ] = []

    def query(
        self,
        query: str,
        *,
        job_config: (
            bigquery.QueryJobConfig
            | None
        ) = None,
        location: str | None = None,
    ) -> FakeQueryJob:
        self.calls.append(
            (
                query,
                job_config,
                location,
            )
        )

        if self.query_error is not None:
            raise self.query_error

        return FakeQueryJob(
            rows=self.rows,
            error=self.result_error,
        )


def make_service(
    client: FakeBigQueryClient,
) -> BigQueryAnalyticsService:
    return BigQueryAnalyticsService(
        project_id="riverwatch-test",
        dataset_id=(
            "riverwatch_dev_analytics"
        ),
        location="asia-south1",
        client=client,
    )


def station_row(
) -> dict[str, object]:
    return {
        "station_id": "44",
        "station_name": "Station A",
        "basin_name": "Koshi",
        "latitude": 27.5,
        "longitude": 85.3,
        "elevation_m": 120.0,
        "source_record_id": (
            "station-snapshot:44"
        ),
        "observed_at": OBSERVED_AT,
        "water_level_m": 2.5,
        "observation_ingested_at": (
            INGESTED_AT
        ),
        "has_observation": True,
        "observation_age_hours": (
            5.0 / 60.0
        ),
    }


def observation_row(
) -> dict[str, object]:
    return {
        "source_record_id": "record-1",
        "station_id": "44",
        "observed_at": OBSERVED_AT,
        "water_level_m": 2.5,
        "endpoint": "river",
        "run_id": "run-123",
        "ingested_at": INGESTED_AT,
    }


def basin_row(
) -> dict[str, object]:
    return {
        "basin_name": "Koshi",
        "total_stations": 10,
        "stations_with_observation": 9,
        "stations_without_observation": 1,
        "fresh_observations": 7,
        "stale_observations": 2,
        "future_observations": 0,
        "unassessable_observations": 0,
        "observations_with_water_level": 8,
        "observations_without_water_level": 1,
        "coverage_ratio": 0.9,
        "freshness_ratio": (
            7.0 / 9.0
        ),
        "oldest_observed_at": (
            OBSERVED_AT
        ),
        "latest_observed_at": (
            INGESTED_AT
        ),
    }


def network_row(
) -> dict[str, object]:
    return {
        "basins_represented": 2,
        "total_stations": 20,
        "stations_with_observation": 18,
        "stations_without_observation": 2,
        "fresh_observations": 14,
        "stale_observations": 4,
        "future_observations": 0,
        "unassessable_observations": 0,
        "observations_with_water_level": 16,
        "observations_without_water_level": 2,
        "oldest_observed_at": (
            OBSERVED_AT
        ),
        "latest_observed_at": (
            INGESTED_AT
        ),
        "coverage_ratio": 0.9,
        "freshness_ratio": (
            14.0 / 18.0
        ),
    }


def test_get_current_snapshot() -> None:
    client = FakeBigQueryClient(
        rows=[
            station_row(),
        ]
    )

    result = make_service(
        client
    ).get_current_snapshot()

    assert result == (
        CurrentRiverStation(
            station_id="44",
            station_name="Station A",
            basin_name="Koshi",
            latitude=27.5,
            longitude=85.3,
            elevation_m=120.0,
            source_record_id=(
                "station-snapshot:44"
            ),
            observed_at=OBSERVED_AT,
            water_level_m=2.5,
            observation_ingested_at=(
                INGESTED_AT
            ),
            has_observation=True,
            observation_age_hours=(
                5.0 / 60.0
            ),
        ),
    )

    query, job_config, location = (
        client.calls[0]
    )

    assert (
        "`riverwatch-test."
        "riverwatch_dev_analytics."
        "current_river_snapshot`"
        in query
    )

    assert job_config is None
    assert location == "asia-south1"


def test_get_station_uses_parameter() -> None:
    client = FakeBigQueryClient(
        rows=[
            station_row(),
        ]
    )

    result = make_service(
        client
    ).get_station(
        " 44 "
    )

    assert result is not None
    assert result.station_id == "44"

    query, job_config, _ = (
        client.calls[0]
    )

    assert (
        "station_id = @station_id"
        in query
    )

    assert job_config is not None

    parameters = (
        job_config.query_parameters
    )

    assert len(parameters) == 1

    parameter = parameters[0]

    assert parameter.name == (
        "station_id"
    )

    assert parameter.value == "44"


def test_get_station_returns_none() -> None:
    service = make_service(
        FakeBigQueryClient()
    )

    assert (
        service.get_station(
            "999"
        )
        is None
    )


def test_blank_station_id_is_rejected() -> None:
    client = FakeBigQueryClient()

    with pytest.raises(
        InvalidAnalyticsQueryError,
        match="station_id must not be blank",
    ):
        make_service(
            client
        ).get_station(
            "   "
        )

    assert client.calls == []


def test_get_station_history_with_window(
) -> None:
    client = FakeBigQueryClient(
        rows=[
            observation_row(),
        ]
    )

    start_at = datetime(
        2026,
        10,
        5,
        0,
        0,
        tzinfo=UTC,
    )

    end_at = datetime(
        2026,
        10,
        6,
        0,
        0,
        tzinfo=UTC,
    )

    result = make_service(
        client
    ).get_station_history(
        "44",
        start_at=start_at,
        end_at=end_at,
    )

    assert result == (
        StationObservation(
            source_record_id="record-1",
            station_id="44",
            observed_at=OBSERVED_AT,
            water_level_m=2.5,
            endpoint="river",
            run_id="run-123",
            ingested_at=INGESTED_AT,
        ),
    )

    query, job_config, _ = (
        client.calls[0]
    )

    assert (
        "`riverwatch-test."
        "riverwatch_dev_analytics."
        "station_observation_history`"
        in query
    )

    assert (
        "observed_at >= @start_at"
        in query
    )

    assert (
        "observed_at < @end_at"
        in query
    )

    assert job_config is not None

    parameters = {
        parameter.name:
            parameter.value
        for parameter
        in job_config.query_parameters
    }

    assert parameters == {
        "station_id": "44",
        "start_at": start_at,
        "end_at": end_at,
    }


def test_invalid_history_window_is_rejected(
) -> None:
    client = FakeBigQueryClient()

    moment = datetime(
        2026,
        10,
        5,
        tzinfo=UTC,
    )

    with pytest.raises(
        InvalidAnalyticsQueryError,
        match=(
            "start_at must be earlier "
            "than end_at"
        ),
    ):
        make_service(
            client
        ).get_station_history(
            "44",
            start_at=moment,
            end_at=moment,
        )

    assert client.calls == []


def test_get_basin_summaries() -> None:
    result = make_service(
        FakeBigQueryClient(
            rows=[
                basin_row(),
            ]
        )
    ).get_basin_summaries()

    assert result == (
        BasinCurrentSummary(
            basin_name="Koshi",
            total_stations=10,
            stations_with_observation=9,
            stations_without_observation=1,
            fresh_observations=7,
            stale_observations=2,
            future_observations=0,
            unassessable_observations=0,
            observations_with_water_level=8,
            observations_without_water_level=1,
            coverage_ratio=0.9,
            freshness_ratio=(
                7.0 / 9.0
            ),
            oldest_observed_at=(
                OBSERVED_AT
            ),
            latest_observed_at=(
                INGESTED_AT
            ),
        ),
    )


def test_get_network_summary() -> None:
    result = make_service(
        FakeBigQueryClient(
            rows=[
                network_row(),
            ]
        )
    ).get_network_summary()

    assert result == (
        CurrentNetworkSummary(
            basins_represented=2,
            total_stations=20,
            stations_with_observation=18,
            stations_without_observation=2,
            fresh_observations=14,
            stale_observations=4,
            future_observations=0,
            unassessable_observations=0,
            observations_with_water_level=16,
            observations_without_water_level=2,
            oldest_observed_at=(
                OBSERVED_AT
            ),
            latest_observed_at=(
                INGESTED_AT
            ),
            coverage_ratio=0.9,
            freshness_ratio=(
                14.0 / 18.0
            ),
        )
    )


def test_network_summary_requires_row(
) -> None:
    with pytest.raises(
        RuntimeError,
        match=(
            "current_network_summary "
            "returned no row"
        ),
    ):
        make_service(
            FakeBigQueryClient()
        ).get_network_summary()


def test_query_failure_is_mapped() -> None:
    error = ServiceUnavailable(  # type: ignore[no-untyped-call]
        "BigQuery unavailable"
    )

    with pytest.raises(
        BigQueryAnalyticsError,
        match=(
            "BigQuery analytics query failed"
        ),
    ):
        make_service(
            FakeBigQueryClient(
                query_error=error
            )
        ).get_current_snapshot()


def test_result_failure_is_mapped() -> None:
    error = ServiceUnavailable(  # type: ignore[no-untyped-call]
        "BigQuery unavailable"
    )

    with pytest.raises(
        BigQueryAnalyticsError,
        match=(
            "BigQuery analytics query failed"
        ),
    ):
        make_service(
            FakeBigQueryClient(
                result_error=error
            )
        ).get_current_snapshot()


@pytest.mark.parametrize(
    (
        "project_id",
        "dataset_id",
        "location",
        "message",
    ),
    [
        (
            "bad project",
            "riverwatch_dev_analytics",
            "asia-south1",
            "project_id contains invalid",
        ),
        (
            "riverwatch-test",
            "bad-dataset",
            "asia-south1",
            "dataset_id contains invalid",
        ),
        (
            "riverwatch-test",
            "riverwatch_dev_analytics",
            " ",
            "location must not be blank",
        ),
    ],
)
def test_rejects_invalid_configuration(
    project_id: str,
    dataset_id: str,
    location: str,
    message: str,
) -> None:
    with pytest.raises(
        ValueError,
        match=message,
    ):
        BigQueryAnalyticsService(
            project_id=project_id,
            dataset_id=dataset_id,
            location=location,
            client=FakeBigQueryClient(),
        )

def close(
    self,
) -> None:
    pass