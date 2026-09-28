import json
from pathlib import Path

from riverwatch.ingestion.bipad.schemas import (
    BipadRiverRecord,
)

FIXTURE_PATH = Path(
    "tests/fixtures/bipad/river_record.json"
)


def load_fixture() -> dict[str, object]:
    return json.loads(
        FIXTURE_PATH.read_text(
            encoding="utf-8",
        )
    )


def test_bipad_river_record_parses_real_payload() -> None:
    payload = load_fixture()

    record = BipadRiverRecord.model_validate(
        payload
    )

    assert record.id == 18707479

    assert record.station == 44

    assert record.station_series_id == 19260

    assert record.title == "Duduwa River(364)"

    assert record.basin == "West Rapti"

    assert record.water_level == 0.201000213623

    assert record.warning_level == 5.5

    assert record.danger_level == 6.0


def test_geojson_coordinate_order() -> None:
    payload = load_fixture()

    record = BipadRiverRecord.model_validate(
        payload
    )

    assert record.point.longitude == 81.697579

    assert record.point.latitude == 28.202405


def test_source_timestamps_are_parsed() -> None:
    payload = load_fixture()

    record = BipadRiverRecord.model_validate(
        payload
    )

    assert record.water_level_on.tzinfo is not None

    assert record.created_on.tzinfo is not None