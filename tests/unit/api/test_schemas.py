from datetime import (
    UTC,
    datetime,
)

import pytest
from pydantic import ValidationError

from riverwatch.analytics.model import (
    BasinCurrentSummary,
    CurrentNetworkSummary,
    CurrentRiverStation,
    StationObservation,
)
from riverwatch.api.schemas.observations import (
    StationObservationResponse,
)
from riverwatch.api.schemas.stations import (
    CurrentRiverStationResponse,
)
from riverwatch.api.schemas.summaries import (
    BasinCurrentSummaryResponse,
    CurrentNetworkSummaryResponse,
)


def test_current_station_response_from_analytics_model() -> None:
    station = CurrentRiverStation(
        station_id="44",
        station_name="Station A",
        basin_name="Koshi",
        latitude=27.7,
        longitude=85.3,
        elevation_m=500.0,
        source_record_id="current-44",
        observed_at=datetime(
            2026,
            9,
            30,
            9,
            30,
            tzinfo=UTC,
        ),
        water_level_m=1.5,
        observation_ingested_at=datetime(
            2026,
            9,
            30,
            10,
            0,
            tzinfo=UTC,
        ),
        has_observation=True,
        observation_age_hours=0.5,
    )

    response = (
        CurrentRiverStationResponse
        .model_validate(
            station
        )
    )

    assert response.station_id == "44"
    assert response.station_name == (
        "Station A"
    )

    assert response.water_level_m == 1.5
    assert response.has_observation

    serialized = response.model_dump(
        mode="json"
    )

    assert serialized[
        "observed_at"
    ] == "2026-09-30T09:30:00Z"

    assert serialized[
        "observation_ingested_at"
    ] == "2026-09-30T10:00:00Z"


def test_station_observation_response_from_analytics_model() -> None:
    observation = StationObservation(
        source_record_id="100",
        station_id="44",
        observed_at=datetime(
            2026,
            9,
            30,
            9,
            30,
            tzinfo=UTC,
        ),
        water_level_m=1.5,
        endpoint="river-stations",
        run_id="current",
        ingested_at=datetime(
            2026,
            9,
            30,
            10,
            0,
            tzinfo=UTC,
        ),
    )

    response = (
        StationObservationResponse
        .model_validate(
            observation
        )
    )

    assert response.station_id == "44"

    assert (
        response.source_record_id
        == "100"
    )

    assert (
        response.observed_at.tzinfo
        is UTC
    )

    assert response.endpoint == (
        "river-stations"
    )


def test_summary_responses_from_analytics_models() -> None:
    oldest = datetime(
        2026,
        9,
        29,
        8,
        0,
        tzinfo=UTC,
    )

    latest = datetime(
        2026,
        9,
        30,
        9,
        30,
        tzinfo=UTC,
    )

    basin = BasinCurrentSummary(
        basin_name="Koshi",
        total_stations=50,
        stations_with_observation=49,
        stations_without_observation=1,
        fresh_observations=30,
        stale_observations=19,
        future_observations=0,
        unassessable_observations=0,
        observations_with_water_level=49,
        observations_without_water_level=0,
        coverage_ratio=0.98,
        freshness_ratio=30 / 49,
        oldest_observed_at=oldest,
        latest_observed_at=latest,
    )

    network = CurrentNetworkSummary(
        basins_represented=25,
        total_stations=284,
        stations_with_observation=279,
        stations_without_observation=5,
        fresh_observations=188,
        stale_observations=91,
        future_observations=0,
        unassessable_observations=0,
        observations_with_water_level=275,
        observations_without_water_level=4,
        oldest_observed_at=oldest,
        latest_observed_at=latest,
        coverage_ratio=279 / 284,
        freshness_ratio=188 / 279,
    )

    basin_response = (
        BasinCurrentSummaryResponse
        .model_validate(
            basin
        )
    )

    network_response = (
        CurrentNetworkSummaryResponse
        .model_validate(
            network
        )
    )

    assert basin_response.basin_name == (
        "Koshi"
    )

    assert (
        basin_response.total_stations
        == 50
    )

    assert (
        network_response.total_stations
        == 284
    )

    assert (
        network_response
        .stations_without_observation
        == 5
    )


def test_response_schema_rejects_naive_datetime() -> None:
    with pytest.raises(
        ValidationError
    ):
        StationObservationResponse(
            source_record_id="100",
            station_id="44",
            observed_at=datetime(
                2026,
                9,
                30,
                9,
                30,
            ),
            water_level_m=1.5,
            endpoint="river",
            run_id="history",
            ingested_at=None,
        )

