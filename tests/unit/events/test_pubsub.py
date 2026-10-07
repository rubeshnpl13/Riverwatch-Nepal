from __future__ import annotations

import json
from concurrent.futures import (
    TimeoutError as FutureTimeoutError,
)
from dataclasses import dataclass, field
from datetime import (
    UTC,
    datetime,
)
from uuid import UUID

import pytest
from google.api_core.exceptions import (
    ServiceUnavailable,
)

from riverwatch.events.bus import (
    EventPublisher,
)
from riverwatch.events.model import (
    EventEnvelope,
    EventType,
)
from riverwatch.events.pubsub import (
    PubSubEventPublisher,
    PubSubPublishError,
)

EVENT_ID = UUID(
    "11111111-1111-1111-1111-111111111111"
)

CORRELATION_ID = UUID(
    "22222222-2222-2222-2222-222222222222"
)

TOPIC = (
    "projects/riverwatch-test/"
    "topics/riverwatch-dev-ingestion-completed"
)


def make_event() -> EventEnvelope:
    return EventEnvelope(
        event_id=EVENT_ID,
        event_type=(
            EventType.INGESTION_COMPLETED
        ),
        occurred_at=datetime(
            2026,
            10,
            5,
            0,
            0,
            tzinfo=UTC,
        ),
        correlation_id=(
            CORRELATION_ID
        ),
        data={
            "provider": "bipad",
            "endpoint": "river-stations",
            "run_id": "run-123",
            "manifest_path": (
                "manifests/run-123/"
                "manifest.json"
            ),
        },
    )


@dataclass
class FakePublishFuture:
    message_id: str = "message-123"
    error: Exception | None = None
    timeouts: list[
        float | None
    ] = field(
        default_factory=list
    )

    def result(
        self,
        timeout: float | None = None,
    ) -> str:
        self.timeouts.append(
            timeout
        )

        if self.error is not None:
            raise self.error

        return self.message_id


@dataclass
class PublishCall:
    topic: str
    data: bytes
    attributes: dict[str, str]


class FakePublisherClient:
    def __init__(
        self,
        *,
        future: FakePublishFuture | None = None,
    ) -> None:
        self.future = (
            future
            if future is not None
            else FakePublishFuture()
        )

        self.calls: list[
            PublishCall
        ] = []

    def publish(
        self,
        topic: str,
        data: bytes,
        **attributes: str,
    ) -> FakePublishFuture:
        self.calls.append(
            PublishCall(
                topic=topic,
                data=data,
                attributes=dict(
                    attributes
                ),
            )
        )

        return self.future


def test_publish_serializes_event_and_waits_for_ack() -> None:
    client = FakePublisherClient()

    publisher = PubSubEventPublisher(
        topic=TOPIC,
        client=client,
        publish_timeout_seconds=12,
    )

    publisher.publish(
        make_event()
    )

    assert len(
        client.calls
    ) == 1

    call = client.calls[0]

    assert call.topic == TOPIC

    payload = json.loads(
        call.data.decode(
            "utf-8"
        )
    )

    assert payload == {
        "event_id": str(
            EVENT_ID
        ),
        "event_type": (
            "ingestion.completed"
        ),
        "occurred_at": (
            "2026-10-05T00:00:00Z"
        ),
        "correlation_id": str(
            CORRELATION_ID
        ),
        "schema_version": 1,
        "data": {
            "provider": "bipad",
            "endpoint": "river-stations",
            "run_id": "run-123",
            "manifest_path": (
                "manifests/run-123/"
                "manifest.json"
            ),
        },
    }

    assert call.attributes == {
        "event_id": str(
            EVENT_ID
        ),
        "event_type": (
            "ingestion.completed"
        ),
        "correlation_id": str(
            CORRELATION_ID
        ),
        "schema_version": "1",
    }

    assert (
        client.future.timeouts
        == [12]
    )


def test_publish_maps_google_api_error() -> None:
    error = ServiceUnavailable(  # type: ignore[no-untyped-call]
        "Pub/Sub unavailable"
    )

    publisher = PubSubEventPublisher(
        topic=TOPIC,
        client=FakePublisherClient(
            future=FakePublishFuture(
                error=error
            )
        ),
    )

    with pytest.raises(
        PubSubPublishError,
        match="Unable to publish",
    ):
        publisher.publish(
            make_event()
        )


def test_publish_maps_timeout() -> None:
    publisher = PubSubEventPublisher(
        topic=TOPIC,
        client=FakePublisherClient(
            future=FakePublishFuture(
                error=(
                    FutureTimeoutError()
                )
            )
        ),
    )

    with pytest.raises(
        PubSubPublishError,
        match="Unable to publish",
    ):
        publisher.publish(
            make_event()
        )


@pytest.mark.parametrize(
    "topic",
    [
        "",
        "   ",
        "ingestion-completed",
        "projects/test",
        "projects/test/topics",
        "projects//topics/test",
        "projects/test/topics/",
    ],
)
def test_rejects_invalid_topic(
    topic: str,
) -> None:
    with pytest.raises(
        ValueError
    ):
        PubSubEventPublisher(
            topic=topic,
            client=FakePublisherClient(),
        )


def test_rejects_non_positive_publish_timeout() -> None:
    with pytest.raises(
        ValueError,
        match="greater than zero",
    ):
        PubSubEventPublisher(
            topic=TOPIC,
            client=FakePublisherClient(),
            publish_timeout_seconds=0,
        )


def test_satisfies_event_publisher_contract() -> None:
    client = FakePublisherClient()

    publisher: EventPublisher = (
        PubSubEventPublisher(
            topic=TOPIC,
            client=client,
        )
    )

    publisher.publish(
        make_event()
    )

    assert len(
        client.calls
    ) == 1