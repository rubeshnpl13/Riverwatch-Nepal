from dataclasses import dataclass
from datetime import datetime


@dataclass(
    frozen=True,
    slots=True,
)
class CurrentRiverStation:
    station_id: str | None
    station_name: str | None
    basin_name: str | None

    latitude: float | None
    longitude: float | None
    elevation_m: float | None

    source_record_id: str | None
    observed_at: datetime | None
    water_level_m: float | None

    observation_ingested_at: (
        datetime | None
    )

    has_observation: bool

    observation_age_hours: (
        float | None
    )


@dataclass(
    frozen=True,
    slots=True,
)
class StationObservation:
    source_record_id: str | None
    station_id: str
    observed_at: datetime
    water_level_m: float | None

    endpoint: str
    run_id: str
    ingested_at: datetime | None


@dataclass(
    frozen=True,
    slots=True,
)
class BasinCurrentSummary:
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
        datetime | None
    )

    latest_observed_at: (
        datetime | None
    )


@dataclass(
    frozen=True,
    slots=True,
)
class CurrentNetworkSummary:
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
        datetime | None
    )

    latest_observed_at: (
        datetime | None
    )

    coverage_ratio: float
    freshness_ratio: float