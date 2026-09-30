from pydantic import AwareDatetime

from riverwatch.api.schemas.base import (
    APIResponseModel,
)


class BasinCurrentSummaryResponse(
    APIResponseModel
):
    basin_name: str

    total_stations: int

    stations_with_observation: int
    stations_without_observation: int

    fresh_observations: int
    stale_observations: int
    future_observations: int
    unassessable_observations: int

    observations_with_water_level: int
    observations_without_water_level: int

    coverage_ratio: float
    freshness_ratio: float

    oldest_observed_at: (
        AwareDatetime | None
    )

    latest_observed_at: (
        AwareDatetime | None
    )


class CurrentNetworkSummaryResponse(
    APIResponseModel
):
    basins_represented: int

    total_stations: int

    stations_with_observation: int
    stations_without_observation: int

    fresh_observations: int
    stale_observations: int
    future_observations: int
    unassessable_observations: int

    observations_with_water_level: int
    observations_without_water_level: int

    oldest_observed_at: (
        AwareDatetime | None
    )

    latest_observed_at: (
        AwareDatetime | None
    )

    coverage_ratio: float
    freshness_ratio: float