from datetime import (
    UTC,
    datetime,
    timedelta,
    timezone,
)
from pathlib import Path
from uuid import UUID

from riverwatch.events.processing_retry import (
    LocalProcessingRetryStore,
)

INGESTION_EVENT_ID = UUID(
    "11111111-1111-1111-1111-111111111111"
)

FIRST_FAILURE_TIME = datetime(
    2026,
    10,
    3,
    0,
    1,
    tzinfo=UTC,
)


def test_missing_retry_state_returns_none(
    tmp_path: Path,
) -> None:
    store = LocalProcessingRetryStore(
        events_root=tmp_path,
    )

    assert (
        store.get(INGESTION_EVENT_ID)
        is None
    )


def test_first_failure_creates_retry_state(
    tmp_path: Path,
) -> None:
    store = LocalProcessingRetryStore(
        events_root=tmp_path,
    )

    state = store.record_failure(
        ingestion_event_id=INGESTION_EVENT_ID,
        failed_at=FIRST_FAILURE_TIME,
        error_type="RuntimeError",
        error_message="processing failed",
    )

    assert state.attempts == 1
    assert (
        state.last_error_type
        == "RuntimeError"
    )
    assert (
        state.last_error_message
        == "processing failed"
    )


def test_failure_increments_attempt_count(
    tmp_path: Path,
) -> None:
    store = LocalProcessingRetryStore(
        events_root=tmp_path,
    )

    store.record_failure(
        ingestion_event_id=INGESTION_EVENT_ID,
        failed_at=FIRST_FAILURE_TIME,
        error_type="RuntimeError",
        error_message="processing failed",
    )

    state = store.record_failure(
        ingestion_event_id=INGESTION_EVENT_ID,
        failed_at=FIRST_FAILURE_TIME,
        error_type="ValueError",
        error_message="second failure",
    )

    assert state.attempts == 2
    assert (
        state.last_error_type
        == "ValueError"
    )
    assert (
        state.last_error_message
        == "second failure"
    )



def test_retry_state_survives_new_store_instance(
    tmp_path: Path,
) -> None:
    first_store = LocalProcessingRetryStore(
        events_root=tmp_path,
    )

    first_store.record_failure(
        ingestion_event_id=INGESTION_EVENT_ID,
        failed_at=FIRST_FAILURE_TIME,
        error_type="RuntimeError",
        error_message="processing failed",
    )

    second_store = LocalProcessingRetryStore(
        events_root=tmp_path,
    )

    state = second_store.get(
        INGESTION_EVENT_ID
    )

    assert state is not None
    assert state.attempts == 1
    assert (
        state.last_error_type
        == "RuntimeError"
    )
    assert (
        state.last_error_message
        == "processing failed"
    )


def test_clear_removes_retry_state(
    tmp_path: Path,
) -> None:
    store = LocalProcessingRetryStore(
        events_root=tmp_path,
    )

    store.record_failure(
        ingestion_event_id=INGESTION_EVENT_ID,
        failed_at=FIRST_FAILURE_TIME,
        error_type="RuntimeError",
        error_message="processing failed",
    )

    store.clear(
        INGESTION_EVENT_ID
    )

    assert (
        store.get(INGESTION_EVENT_ID)
        is None
    )

def test_retry_state_normalizes_timezone(
    tmp_path: Path,
) -> None:
    store = LocalProcessingRetryStore(
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

    state = store.record_failure(
        ingestion_event_id=INGESTION_EVENT_ID,
        failed_at=non_utc_time,
        error_type="RuntimeError",
        error_message="processing failed",
    )

    assert (
        state.last_failed_at
        == datetime(
            2026,
            10,
            3,
            0,
            0,
            tzinfo=UTC,
        )
    )