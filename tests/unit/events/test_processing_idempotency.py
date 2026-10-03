from datetime import (
    UTC,
    datetime,
    timedelta,
    timezone,
)
from pathlib import Path
from uuid import UUID

import pytest

from riverwatch.events.processing_idempotency import (
    LocalProcessingExecutionStore,
    ProcessingExecutionConflictError,
    ProcessingExecutionRecord,
)

INGESTION_EVENT_ID = UUID(
    "11111111-1111-1111-1111-111111111111"
)

PROCESSING_EVENT_ID = UUID(
    "22222222-2222-2222-2222-222222222222"
)

CORRELATION_ID = UUID(
    "33333333-3333-3333-3333-333333333333"
)

REQUEST_EVENT_ID = UUID(
    "44444444-4444-4444-4444-444444444444"
)

PROCESSING_TIME = datetime(
    2026,
    10,
    3,
    0,
    1,
    tzinfo=UTC,
)


def make_record(
    *,
    occurred_at: datetime = PROCESSING_TIME,
    completion_event_id: UUID = PROCESSING_EVENT_ID,
) -> ProcessingExecutionRecord:
    return ProcessingExecutionRecord(
        ingestion_event_id=INGESTION_EVENT_ID,
        completion_event_id=completion_event_id,
        correlation_id=CORRELATION_ID,
        request_event_id=REQUEST_EVENT_ID,
        provider="bipad",
        endpoint="river-stations",
        run_id="run-123",
        manifest_path=(
            "manifests/"
            "run-123/"
            "manifest.json"
        ),
        quality_report_path=(
            "quality/"
            "run-123/"
            "report.json"
        ),
        station_output_path=(
            "processed/stations/"
            "run-123"
        ),
        observation_output_path=(
            "processed/observations/"
            "run-123"
        ),
        occurred_at=occurred_at,
    )


def test_missing_record_returns_none(
    tmp_path: Path,
) -> None:
    store = LocalProcessingExecutionStore(
        events_root=tmp_path,
    )

    assert (
        store.get(INGESTION_EVENT_ID)
        is None
    )


def test_save_and_load_round_trip(
    tmp_path: Path,
) -> None:
    store = LocalProcessingExecutionStore(
        events_root=tmp_path,
    )

    record = make_record()

    store.save(record)

    loaded = store.get(
        INGESTION_EVENT_ID
    )

    assert loaded == record


def test_saving_same_record_twice_is_noop(
    tmp_path: Path,
) -> None:
    store = LocalProcessingExecutionStore(
        events_root=tmp_path,
    )

    record = make_record()

    store.save(record)
    store.save(record)

    loaded = store.get(
        INGESTION_EVENT_ID
    )

    assert loaded == record


def test_conflicting_record_raises_conflict_error(
    tmp_path: Path,
) -> None:
    store = LocalProcessingExecutionStore(
        events_root=tmp_path,
    )

    first = make_record()

    conflicting = make_record(
        completion_event_id=UUID(
            "55555555-5555-5555-5555-555555555555"
        )
    )

    store.save(first)

    with pytest.raises(
            ProcessingExecutionConflictError
    ):
        store.save(conflicting)


def test_record_normalizes_timezone_to_utc(
    tmp_path: Path,
) -> None:
    store = LocalProcessingExecutionStore(
        events_root=tmp_path,
    )

    non_utc_time = datetime(
        2026,
        10,
        3,
        5,
        45,
        tzinfo=timezone(
            timedelta(
                hours=5,
                minutes=45,
            )
        ),
    )

    record = make_record(
        occurred_at=non_utc_time
    )

    store.save(record)

    loaded = store.get(
        INGESTION_EVENT_ID
    )

    assert loaded is not None

    assert (
        loaded.occurred_at
        == datetime(
            2026,
            10,
            3,
            0,
            0,
            tzinfo=UTC,
        )
    )