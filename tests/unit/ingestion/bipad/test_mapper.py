import json
from datetime import UTC, datetime
from pathlib import Path

from riverwatch.ingestion.bipad.mapper import (
    map_bipad_river_record,
)
from riverwatch.ingestion.bipad.schemas import (
    BipadRiverRecord,
)
from riverwatch.models import RiverTrend

FIXTURE_PATH = Path(
    "tests/fixtures/bipad/river_record.json"
)


def test_maps_real_bipad_record() -> None:
    payload = json.loads(
        FIXTURE_PATH.read_text(
            encoding="utf-8",
        )
    )

    source = BipadRiverRecord.model_validate(
        payload
    )

    ingestion_time = datetime(
        2026,
        9,
        28,
        9,
        5,
        tzinfo=UTC,
    )

    observation = map_bipad_river_record(
        source,
        ingested_at=ingestion_time,
    )

    assert observation.station_id == "44"

    assert observation.source_record_id == "18707479"

    assert (
        observation.source_station_series_id
        == "19260"
    )

    assert observation.station_name == "Duduwa River(364)"

    assert observation.basin_name == "West Rapti"

    assert observation.longitude == 81.697579

    assert observation.latitude == 28.202405

    assert observation.water_level_m == 0.201000213623

    assert observation.warning_level_m == 5.5

    assert observation.danger_level_m == 6.0

    assert observation.trend == RiverTrend.STABLE

    assert observation.ingested_at == ingestion_time