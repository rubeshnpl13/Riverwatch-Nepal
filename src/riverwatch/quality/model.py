from dataclasses import dataclass
from datetime import (
    datetime,
    timedelta,
)
from enum import StrEnum

from riverwatch.ingestion.bipad.endpoints import (
    BipadEndpoint,
)


class QualityStatus(StrEnum):
    PASS = "pass"
    WARN = "warn"
    FAIL = "fail"


class ObservationQualityRule(StrEnum):
    MISSING_SOURCE_RECORD_ID = (
        "missing_source_record_id"
    )

    MISSING_STATION_ID = (
        "missing_station_id"
    )

    MISSING_OBSERVED_AT = (
        "missing_observed_at"
    )

    MISSING_RUN_ID = (
        "missing_run_id"
    )

    INVALID_LATITUDE = (
        "invalid_latitude"
    )

    INVALID_LONGITUDE = (
        "invalid_longitude"
    )

    MISSING_WATER_LEVEL = (
        "missing_water_level"
    )

    MISSING_COORDINATES = (
        "missing_coordinates"
    )

    SUSPICIOUS_WATER_LEVEL = (
        "suspicious_water_level"
    )

    WARNING_ABOVE_DANGER = (
        "warning_above_danger"
    )

    STALE_CURRENT_OBSERVATION = (
        "stale_current_observation"
    )

    DUPLICATE_SOURCE_RECORD_ID = (
        "duplicate_source_record_id"
    )

    DUPLICATE_STATION_OBSERVED_AT = (
        "duplicate_station_observed_at"
    )


class StationQualityRule(StrEnum):
    MISSING_STATION_ID = (
        "missing_station_id"
    )

    MISSING_STATION_NAME = (
        "missing_station_name"
    )

    MISSING_RUN_ID = (
        "missing_run_id"
    )

    INVALID_LATITUDE = (
        "invalid_latitude"
    )

    INVALID_LONGITUDE = (
        "invalid_longitude"
    )

    MISSING_STATION_SERIES_ID = (
        "missing_station_series_id"
    )

    MISSING_BASIN_NAME = (
        "missing_basin_name"
    )

    MISSING_COORDINATES = (
        "missing_coordinates"
    )

    DUPLICATE_STATION_ID = (
        "duplicate_station_id"
    )

    DUPLICATE_STATION_SERIES_ID = (
        "duplicate_station_series_id"
    )


@dataclass(
    frozen=True,
)
class ObservationQualityPolicy:
    current_max_age: timedelta = (
        timedelta(
            hours=24,
        )
    )

    suspicious_absolute_water_level_m: float = (
        10_000.0
    )

    def __post_init__(
        self,
    ) -> None:
        if (
            self.current_max_age
            <= timedelta(0)
        ):
            raise ValueError(
                "current_max_age must be positive"
            )

        if (
            self.suspicious_absolute_water_level_m
            <= 0
        ):
            raise ValueError(
                "suspicious_absolute_water_level_m "
                "must be positive"
            )


DEFAULT_OBSERVATION_QUALITY_POLICY = (
    ObservationQualityPolicy()
)


@dataclass(
    frozen=True,
)
class CurrentDataHealthSummary:
    total_stations: int

    stations_with_observation: int

    stations_without_observation: int

    total_observations: int

    fresh_observations: int

    stale_observations: int

    future_observations: int

    unassessable_observations: int

    coverage_ratio: float

    freshness_ratio: float

    oldest_observed_at: (
        datetime | None
    )

    latest_observed_at: (
        datetime | None
    )

    status: QualityStatus


class QualityDataset(StrEnum):
    STATIONS = "stations"
    OBSERVATIONS = "observations"


@dataclass(
    frozen=True,
)
class QualityRuleCount:
    rule: str
    count: int


@dataclass(
    frozen=True,
)
class DatasetQualitySummary:
    dataset: QualityDataset

    total_rows: int

    pass_rows: int

    warn_rows: int

    fail_rows: int

    affected_rows: int

    pass_ratio: float

    status: QualityStatus

    error_rule_counts: tuple[
        QualityRuleCount,
        ...,
    ]

    warning_rule_counts: tuple[
        QualityRuleCount,
        ...,
    ]

@dataclass(
    frozen=True,
)
class QualityRunReport:
    report_version: int

    run_id: str

    endpoint: BipadEndpoint

    captured_at: datetime

    station_summary: (
        DatasetQualitySummary | None
    )

    observation_summary: (
        DatasetQualitySummary
    )

    current_data_health: (
        CurrentDataHealthSummary | None
    )