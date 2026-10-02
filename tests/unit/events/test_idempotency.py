from datetime import (
    UTC,
    datetime,
    timedelta,
    timezone,
)
from pathlib import Path
from uuid import UUID

import pytest

from riverwatch.events.idempotency import (
    IdempotencyConflictError,
    IngestionExecutionRecord,
    LocalIngestionExecutionStore,
)

REQUEST_EVENT_ID = UUID(
    "11111111-1111-1111-1111-111111111111"
)

COMPLETION_EVENT_ID = UUID(
    "22222222-2222-2222-2222-222222222222"
)

CORRELATION_ID = UUID(
    "33333333-3333-3333-3333-333333333333"
)


def make_record(
    *,
    run_id: str = "run-123",
) -> IngestionExecutionRecord:
    return IngestionExecutionRecord(
        request_event_id=(
            REQUEST_EVENT_ID
        ),
        completion_event_id=(
            COMPLETION_EVENT_ID
        ),
        correlation_id=(
            CORRELATION_ID
        ),
        provider="bipad",
        endpoint="river-stations",
        run_id=run_id,
        manifest_path=(
            "manifests/run-123/"
            "manifest.json"
        ),
        occurred_at=datetime(
            2026,
            10,
            2,
            1,
            0,
            tzinfo=UTC,
        ),
    )


def test_get_returns_none_when_record_missing(
    tmp_path: Path,
) -> None:
    store = (
        LocalIngestionExecutionStore(
            events_root=tmp_path
        )
    )

    assert (
        store.get(
            REQUEST_EVENT_ID
        )
        is None
    )


def test_save_and_load_round_trip(
    tmp_path: Path,
) -> None:
    store = (
        LocalIngestionExecutionStore(
            events_root=tmp_path
        )
    )

    record = make_record()

    store.save(
        record
    )

    assert (
        store.get(
            REQUEST_EVENT_ID
        )
        == record
    )


def test_saving_same_record_is_idempotent(
    tmp_path: Path,
) -> None:
    store = (
        LocalIngestionExecutionStore(
            events_root=tmp_path
        )
    )

    record = make_record()

    store.save(
        record
    )

    store.save(
        record
    )

    assert (
        store.get(
            REQUEST_EVENT_ID
        )
        == record
    )


def test_conflicting_record_is_rejected(
    tmp_path: Path,
) -> None:
    store = (
        LocalIngestionExecutionStore(
            events_root=tmp_path
        )
    )

    store.save(
        make_record(
            run_id="run-123",
        )
    )

    with pytest.raises(
        IdempotencyConflictError,
        match=(
            "Conflicting ingestion "
            "execution record"
        ),
    ):
        store.save(
            make_record(
                run_id="run-456",
            )
        )


def test_record_normalizes_timestamp_to_utc() -> None:
    nepal_time = timezone(
        timedelta(
            hours=5,
            minutes=45,
        )
    )

    record = (
        IngestionExecutionRecord(
            request_event_id=(
                REQUEST_EVENT_ID
            ),
            completion_event_id=(
                COMPLETION_EVENT_ID
            ),
            correlation_id=(
                CORRELATION_ID
            ),
            provider="bipad",
            endpoint="river",
            run_id="run-123",
            manifest_path=(
                "manifest.json"
            ),
            occurred_at=datetime(
                2026,
                10,
                2,
                5,
                45,
                tzinfo=nepal_time,
            ),
        )
    )

    assert (
        record.occurred_at
        == datetime(
            2026,
            10,
            2,
            0,
            0,
            tzinfo=UTC,
        )
    )