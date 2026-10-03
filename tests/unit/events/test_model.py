from datetime import (
    UTC,
    datetime,
    timedelta,
    timezone,
)
from uuid import UUID

import pytest

from riverwatch.events.model import (
    EventEnvelope,
    EventType,
)

EVENT_ID = UUID(
    "11111111-1111-1111-1111-111111111111"
)

CORRELATION_ID = UUID(
    "22222222-2222-2222-2222-222222222222"
)


def test_event_type_values_are_stable() -> None:
    assert (
        EventType.INGESTION_REQUESTED.value
        == "ingestion.requested"
    )

    assert (
        EventType.INGESTION_COMPLETED.value
        == "ingestion.completed"
    )
    assert (
            EventType.PROCESSING_COMPLETED.value
            == "processing.completed"
    )


def test_event_normalizes_timestamp_to_utc() -> None:
    eastern = timezone(
        -timedelta(
            hours=4
        )
    )

    event = EventEnvelope(
        event_id=EVENT_ID,
        event_type=(
            EventType
            .INGESTION_REQUESTED
        ),
        occurred_at=datetime(
            2026,
            10,
            1,
            12,
            0,
            tzinfo=eastern,
        ),
        correlation_id=(
            CORRELATION_ID
        ),
        data={
            "provider": "bipad",
            "endpoint": (
                "river-stations"
            ),
        },
    )

    assert (
        event.occurred_at
        == datetime(
            2026,
            10,
            1,
            16,
            0,
            tzinfo=UTC,
        )
    )


def test_event_rejects_naive_timestamp() -> None:
    with pytest.raises(
        ValueError,
        match=(
            "occurred_at must be "
            "timezone-aware"
        ),
    ):
        EventEnvelope(
            event_id=EVENT_ID,
            event_type=(
                EventType
                .INGESTION_REQUESTED
            ),
            occurred_at=datetime(
                2026,
                10,
                1,
                12,
                0,
            ),
            correlation_id=(
                CORRELATION_ID
            ),
            data={},
        )


def test_event_rejects_invalid_schema_version() -> None:
    with pytest.raises(
        ValueError,
        match=(
            "schema_version must be "
            "at least 1"
        ),
    ):
        EventEnvelope(
            event_id=EVENT_ID,
            event_type=(
                EventType
                .INGESTION_REQUESTED
            ),
            occurred_at=datetime.now(
                UTC
            ),
            correlation_id=(
                CORRELATION_ID
            ),
            data={},
            schema_version=0,
        )


def test_event_copies_input_data() -> None:
    data = {
        "provider": "bipad",
    }

    event = EventEnvelope(
        event_id=EVENT_ID,
        event_type=(
            EventType
            .INGESTION_REQUESTED
        ),
        occurred_at=datetime.now(
            UTC
        ),
        correlation_id=(
            CORRELATION_ID
        ),
        data=data,
    )

    data["provider"] = "changed"

    assert (
        event.data[
            "provider"
        ]
        == "bipad"
    )