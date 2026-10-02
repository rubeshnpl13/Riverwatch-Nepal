from datetime import (
    UTC,
    datetime,
    timedelta,
    timezone,
)
from uuid import UUID

import pytest

from riverwatch.events.ingestion import (
    IngestionRequestPublisher,
)
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

EXPLICIT_CORRELATION_ID = UUID(
    "33333333-3333-3333-3333-333333333333"
)

OCCURRED_AT = datetime(
    2026,
    10,
    2,
    0,
    0,
    tzinfo=UTC,
)


class RecordingPublisher:
    def __init__(
        self,
    ) -> None:
        self.events: list[
            EventEnvelope
        ] = []

    def publish(
        self,
        event: EventEnvelope,
    ) -> None:
        self.events.append(
            event
        )


class UUIDSequence:
    def __init__(
        self,
        *values: UUID,
    ) -> None:
        self._values = iter(
            values
        )

    def __call__(
        self,
    ) -> UUID:
        return next(
            self._values
        )


def test_request_publishes_ingestion_requested_event() -> None:
    publisher = (
        RecordingPublisher()
    )

    service = (
        IngestionRequestPublisher(
            publisher=publisher,
            clock=lambda: (
                OCCURRED_AT
            ),
            uuid_factory=UUIDSequence(
                EVENT_ID,
                CORRELATION_ID,
            ),
        )
    )

    event = service.request(
        provider="bipad",
        endpoint="river-stations",
    )

    assert (
        publisher.events
        == [
            event
        ]
    )

    assert (
        event.event_id
        == EVENT_ID
    )

    assert (
        event.correlation_id
        == CORRELATION_ID
    )

    assert (
        event.event_type
        == EventType
        .INGESTION_REQUESTED
    )

    assert (
        event.occurred_at
        == OCCURRED_AT
    )

    assert (
        dict(
            event.data
        )
        == {
            "provider":
                "bipad",

            "endpoint":
                "river-stations",
        }
    )


def test_request_uses_existing_correlation_id() -> None:
    publisher = (
        RecordingPublisher()
    )

    service = (
        IngestionRequestPublisher(
            publisher=publisher,
            clock=lambda: (
                OCCURRED_AT
            ),
            uuid_factory=UUIDSequence(
                EVENT_ID,
            ),
        )
    )

    event = service.request(
        provider="bipad",
        endpoint="river",
        correlation_id=(
            EXPLICIT_CORRELATION_ID
        ),
    )

    assert (
        event.event_id
        == EVENT_ID
    )

    assert (
        event.correlation_id
        == EXPLICIT_CORRELATION_ID
    )


def test_request_normalizes_provider_and_endpoint() -> None:
    publisher = (
        RecordingPublisher()
    )

    service = (
        IngestionRequestPublisher(
            publisher=publisher,
            clock=lambda: (
                OCCURRED_AT
            ),
            uuid_factory=UUIDSequence(
                EVENT_ID,
                CORRELATION_ID,
            ),
        )
    )

    event = service.request(
        provider="  bipad  ",
        endpoint=(
            "  river-stations  "
        ),
    )

    assert (
        event.data[
            "provider"
        ]
        == "bipad"
    )

    assert (
        event.data[
            "endpoint"
        ]
        == "river-stations"
    )


def test_request_rejects_blank_provider() -> None:
    publisher = (
        RecordingPublisher()
    )

    service = (
        IngestionRequestPublisher(
            publisher=publisher,
        )
    )

    with pytest.raises(
        ValueError,
        match=(
            "provider must not be blank"
        ),
    ):
        service.request(
            provider="   ",
            endpoint=(
                "river-stations"
            ),
        )

    assert (
        publisher.events
        == []
    )


def test_request_rejects_blank_endpoint() -> None:
    publisher = (
        RecordingPublisher()
    )

    service = (
        IngestionRequestPublisher(
            publisher=publisher,
        )
    )

    with pytest.raises(
        ValueError,
        match=(
            "endpoint must not be blank"
        ),
    ):
        service.request(
            provider="bipad",
            endpoint="   ",
        )

    assert (
        publisher.events
        == []
    )


def test_request_preserves_timezone_normalization() -> None:
    publisher = (
        RecordingPublisher()
    )

    non_utc_time = datetime(
        2026,
        10,
        2,
        5,
        45,
        tzinfo=timezone(timedelta(
            hours=5,
            minutes=45,
        )
        ),
    )

    service = (
        IngestionRequestPublisher(
            publisher=publisher,
            clock=lambda: (
                non_utc_time
            ),
            uuid_factory=UUIDSequence(
                EVENT_ID,
                CORRELATION_ID,
            ),
        )
    )

    event = service.request(
        provider="bipad",
        endpoint="river",
    )

    assert (
            event.occurred_at
            == datetime(
        2026,
        10,
        2,
        0,
        0,
        tzinfo=UTC,
    )
    )