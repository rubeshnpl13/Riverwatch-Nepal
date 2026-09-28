import json
from datetime import UTC, datetime
from pathlib import Path

from riverwatch.ingestion.bipad.mapper import (
    map_bipad_station,
    map_bipad_station_observation,
)
from riverwatch.ingestion.bipad.schemas import (
    BipadRiverStation,
)

FIXTURE_PATH = Path(
    "tests/fixtures/bipad/river_station.json"
)


def load_station() -> BipadRiverStation:
    payload = json.loads(
        FIXTURE_PATH.read_text(
            encoding="utf-8"
        )
    )

    return BipadRiverStation.model_validate(
        payload
    )


def test_maps_bipad_station() -> None:
    source = load_station()

    station = map_bipad_station(source)

    assert station.station_id == "282"
    assert station.station_series_id == "36640"

    assert (
        station.station_name
        == "Banara River at EW Highway"
    )

    assert station.basin_name == "Mahakali"

    assert station.longitude == 80.3954179
    assert station.latitude == 28.8886706

    assert station.elevation_m == 221

    assert station.province_id == 7
    assert station.district_id == 75

    assert station.affected_household_count == 90592


def test_station_snapshot_maps_to_observation() -> None:
    source = load_station()

    ingestion_time = datetime(
        2026,
        9,
        28,
        10,
        0,
        tzinfo=UTC,
    )

    observation = map_bipad_station_observation(
        source,
        ingested_at=ingestion_time,
    )

    assert observation is not None

    assert observation.station_id == "282"

    assert observation.water_level_m == 1.215

    assert observation.warning_level_m == 2.8

    assert observation.danger_level_m == 3.2

    assert (
        observation.official_status
        == "BELOW WARNING LEVEL"
    )


def test_snapshot_observation_has_deterministic_id() -> None:
    source = load_station()

    first = map_bipad_station_observation(
        source
    )

    second = map_bipad_station_observation(
        source
    )

    assert first is not None
    assert second is not None

    assert (
        first.source_record_id
        == second.source_record_id
    )