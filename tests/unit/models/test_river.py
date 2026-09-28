from datetime import UTC, datetime

import pytest
from pydantic import ValidationError

from riverwatch.models import (
    DataProvider,
    OriginalDataSource,
    RiverObservation,
    RiverTrend,
)


def test_valid_river_observation() -> None:
    observation = RiverObservation(
        station_id="devghat-001",
        source_record_id="test-observation-001",
        station_name="Devghat",
        river_name="Narayani",
        basin_name="Narayani",
        latitude=27.707,
        longitude=84.426,
        observed_at=datetime(
            2026,
            9,
            28,
            10,
            0,
            tzinfo=UTC,
        ),
        water_level_m=6.42,
        warning_level_m=7.3,
        danger_level_m=8.0,
        trend=RiverTrend.RISING,
        official_status="below_warning",
        provider=DataProvider.BIPAD,
        original_source=OriginalDataSource.DHM,
        ingested_at=datetime.now(UTC),
    )

    assert observation.station_id == "devghat-001"
    assert observation.water_level_m == 6.42
    assert observation.trend == RiverTrend.RISING


def test_invalid_latitude_is_rejected() -> None:
    with pytest.raises(ValidationError):
        RiverObservation(
            station_id="station-001",
            observed_at=datetime.now(UTC),
            latitude=100,
            provider=DataProvider.BIPAD,
            original_source=OriginalDataSource.DHM,
            ingested_at=datetime.now(UTC),
        )


def test_empty_station_id_is_rejected() -> None:
    with pytest.raises(ValidationError):
        RiverObservation(
            station_id="",
            observed_at=datetime.now(UTC),
            provider=DataProvider.BIPAD,
            original_source=OriginalDataSource.DHM,
            ingested_at=datetime.now(UTC),
        )

