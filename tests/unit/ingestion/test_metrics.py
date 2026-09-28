import json
from datetime import UTC, datetime
from pathlib import Path

import pytest
from pydantic import ValidationError

from riverwatch.ingestion.bipad.endpoints import (
    BipadEndpoint,
)
from riverwatch.ingestion.bipad.schemas import (
    BipadRiverStation,
)
from riverwatch.ingestion.bipad.validation import (
    QuarantinedRecord,
    ValidatedRecord,
    validate_records,
)
from riverwatch.ingestion.metrics import (
    IngestionMetrics,
    IngestionRunTracker,
)

FIXTURE_PATH = Path(
    "tests/fixtures/bipad/river_station.json"
)


def load_station_fixture() -> dict[str, object]:
    return json.loads(
        FIXTURE_PATH.read_text(
            encoding="utf-8",
        )
    )

#test 1 run metrics
def test_tracker_builds_ingestion_result() -> None:
    start = datetime(
        2026,
        9,
        28,
        12,
        0,
        tzinfo=UTC,
    )

    finish = datetime(
        2026,
        9,
        28,
        12,
        0,
        1,
        tzinfo=UTC,
    )

    wall_times = iter(
        [
            start,
            finish,
        ]
    )

    monotonic_times = iter(
        [
            10.0,
            10.25,
        ]
    )

    tracker = IngestionRunTracker(
        endpoint=(
            BipadEndpoint.RIVER_STATIONS
        ),
        run_id="test-run-123",
        now=lambda: next(
            wall_times
        ),
        monotonic_clock=lambda: next(
            monotonic_times
        ),
    )

    station = BipadRiverStation.model_validate(
        load_station_fixture()
    )

    tracker.record_validation_result(
        ValidatedRecord(
            record_index=0,
            value=station,
        )
    )

    tracker.record_station_emitted()

    tracker.record_observation_emitted()

    result = tracker.finish()

    assert result.run_id == "test-run-123"

    assert (
        result.endpoint
        == BipadEndpoint.RIVER_STATIONS
    )

    assert result.started_at == start
    assert result.finished_at == finish

    assert result.duration_ms == 250.0

    assert (
        result.metrics.records_received
        == 1
    )

    assert (
        result.metrics.records_valid
        == 1
    )

    assert (
        result.metrics.records_invalid
        == 0
    )

    assert (
        result.metrics.stations_emitted
        == 1
    )

    assert (
        result.metrics.observations_emitted
        == 1
    )

#test quarantine metrics

def test_tracker_counts_quarantined_record() -> None:
    raw_record = load_station_fixture()

    raw_record.pop("point")

    validation_result = list(
        validate_records(
            [raw_record],
            BipadRiverStation,
            endpoint=(
                BipadEndpoint.RIVER_STATIONS
            ),
        )
    )[0]

    assert isinstance(
        validation_result,
        QuarantinedRecord,
    )

    tracker = IngestionRunTracker(
        endpoint=(
            BipadEndpoint.RIVER_STATIONS
        )
    )

    tracker.record_validation_result(
        validation_result
    )

    result = tracker.finish()

    assert (
        result.metrics.records_received
        == 1
    )

    assert (
        result.metrics.records_valid
        == 0
    )

    assert (
        result.metrics.records_invalid
        == 1
    )

    assert (
        len(result.quarantined_records)
        == 1
    )

#test missing observation

def test_tracker_counts_record_without_observation() -> None:
    station = BipadRiverStation.model_validate(
        load_station_fixture()
    )

    tracker = IngestionRunTracker(
        endpoint=(
            BipadEndpoint.RIVER_STATIONS
        )
    )

    tracker.record_validation_result(
        ValidatedRecord(
            record_index=0,
            value=station,
        )
    )

    tracker.record_station_emitted()

    tracker.record_without_observation()

    result = tracker.finish()

    assert (
        result.metrics.records_received
        == 1
    )

    assert (
        result.metrics.stations_emitted
        == 1
    )

    assert (
        result.metrics.observations_emitted
        == 0
    )

    assert (
        result.metrics.records_without_observation
        == 1
    )

#Test invalid metrics cannot exist
def test_metrics_reject_inconsistent_record_totals() -> None:
    with pytest.raises(
        ValidationError
    ):
        IngestionMetrics(
            records_received=10,
            records_valid=8,
            records_invalid=1,
        )

#test - finish cannot happen twice
def test_tracker_cannot_finish_twice() -> None:
    tracker = IngestionRunTracker(
        endpoint=(
            BipadEndpoint.RIVER_STATIONS
        )
    )

    tracker.finish()

    with pytest.raises(
        RuntimeError,
        match="already finished",
    ):
        tracker.finish()

#Test updates after finish
def test_tracker_rejects_updates_after_finish() -> None:
    tracker = IngestionRunTracker(
        endpoint=(
            BipadEndpoint.RIVER_STATIONS
        )
    )

    tracker.finish()

    with pytest.raises(
        RuntimeError,
        match="already finished",
    ):
        tracker.record_station_emitted()