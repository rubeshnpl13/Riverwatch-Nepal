from datetime import (
    UTC,
    datetime,
    timedelta,
)
from pathlib import Path
from uuid import UUID

import pytest

from riverwatch.events.retry import (
    LocalIngestionRetryStore,
    RetryPolicy,
)

REQUEST_EVENT_ID = UUID(
    "11111111-1111-1111-1111-111111111111"
)

FIRST_FAILURE = datetime(
    2026,
    10,
    3,
    0,
    0,
    tzinfo=UTC,
)

SECOND_FAILURE = (
    FIRST_FAILURE
    + timedelta(
        minutes=1
    )
)


def test_retry_policy_rejects_invalid_max_attempts() -> None:
    with pytest.raises(
        ValueError,
        match=(
            "max_attempts must be "
            "at least 1"
        ),
    ):
        RetryPolicy(
            max_attempts=0
        )


def test_get_returns_none_without_retry_state(
    tmp_path: Path,
) -> None:
    store = (
        LocalIngestionRetryStore(
            events_root=tmp_path
        )
    )

    assert (
        store.get(
            REQUEST_EVENT_ID
        )
        is None
    )


def test_record_failure_creates_first_attempt(
    tmp_path: Path,
) -> None:
    store = (
        LocalIngestionRetryStore(
            events_root=tmp_path
        )
    )

    state = store.record_failure(
        request_event_id=(
            REQUEST_EVENT_ID
        ),
        failed_at=FIRST_FAILURE,
        error_type=(
            "RuntimeError"
        ),
        error_message=(
            "ingestion failed"
        ),
    )

    assert state.attempts == 1

    assert (
        state.first_failed_at
        == FIRST_FAILURE
    )

    assert (
        state.last_failed_at
        == FIRST_FAILURE
    )

    assert (
        state.last_error_type
        == "RuntimeError"
    )

    assert (
        state.last_error_message
        == "ingestion failed"
    )


def test_record_failure_increments_attempts(
    tmp_path: Path,
) -> None:
    store = (
        LocalIngestionRetryStore(
            events_root=tmp_path
        )
    )

    store.record_failure(
        request_event_id=(
            REQUEST_EVENT_ID
        ),
        failed_at=FIRST_FAILURE,
        error_type=(
            "RuntimeError"
        ),
        error_message="first",
    )

    state = store.record_failure(
        request_event_id=(
            REQUEST_EVENT_ID
        ),
        failed_at=SECOND_FAILURE,
        error_type=(
            "TimeoutError"
        ),
        error_message="second",
    )

    assert state.attempts == 2

    assert (
        state.first_failed_at
        == FIRST_FAILURE
    )

    assert (
        state.last_failed_at
        == SECOND_FAILURE
    )

    assert (
        state.last_error_type
        == "TimeoutError"
    )

    assert (
        state.last_error_message
        == "second"
    )


def test_retry_state_survives_new_store_instance(
    tmp_path: Path,
) -> None:
    first_store = (
        LocalIngestionRetryStore(
            events_root=tmp_path
        )
    )

    first_store.record_failure(
        request_event_id=(
            REQUEST_EVENT_ID
        ),
        failed_at=FIRST_FAILURE,
        error_type=(
            "RuntimeError"
        ),
        error_message=(
            "ingestion failed"
        ),
    )

    second_store = (
        LocalIngestionRetryStore(
            events_root=tmp_path
        )
    )

    state = second_store.get(
        REQUEST_EVENT_ID
    )

    assert state is not None

    assert state.attempts == 1


def test_clear_removes_retry_state(
    tmp_path: Path,
) -> None:
    store = (
        LocalIngestionRetryStore(
            events_root=tmp_path
        )
    )

    store.record_failure(
        request_event_id=(
            REQUEST_EVENT_ID
        ),
        failed_at=FIRST_FAILURE,
        error_type=(
            "RuntimeError"
        ),
        error_message=(
            "ingestion failed"
        ),
    )

    store.clear(
        REQUEST_EVENT_ID
    )

    assert (
        store.get(
            REQUEST_EVENT_ID
        )
        is None
    )