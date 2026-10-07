from __future__ import annotations

from datetime import (
    UTC,
    datetime,
)
from pathlib import Path
from uuid import UUID

from fastapi.testclient import (
    TestClient,
)

from riverwatch.cloud.processing_completion_relay import (
    CompletionReceiptStore,
)
from riverwatch.cloud.processing_completion_relay_app import (
    create_app,
)
from riverwatch.events.local import (
    LocalEventBus,
)
from riverwatch.events.model import (
    EventEnvelope,
    EventType,
)
from riverwatch.events.serialization import (
    encode_event,
)

COMPLETION_EVENT_ID = UUID(
    "11111111-1111-1111-1111-111111111111"
)

CORRELATION_ID = UUID(
    "22222222-2222-2222-2222-222222222222"
)

INGESTION_EVENT_ID = UUID(
    "33333333-3333-3333-3333-333333333333"
)

REQUEST_EVENT_ID = UUID(
    "44444444-4444-4444-4444-444444444444"
)

COMPLETED_AT = datetime(
    2026,
    10,
    7,
    12,
    0,
    tzinfo=UTC,
)

BUCKET_NAME = (
    "riverwatch-integration-lake"
)

QUALITY_REPORT_KEY = (
    "quality/provider=bipad/"
    "endpoint=river-stations/"
    "run_id=run-123/"
    "report.json"
)

COMPLETION_OBJECT_KEY = (
    "quality/provider=bipad/"
    "endpoint=river-stations/"
    "run_id=run-123/"
    "processing-completed.json"
)


class InMemoryReceiptStore:
    def __init__(
        self,
        *,
        event: EventEnvelope,
    ) -> None:
        self._event = event

    @property
    def bucket_name(
        self,
    ) -> str:
        return BUCKET_NAME

    def read_bytes(
        self,
        *,
        key: str,
    ) -> bytes:
        assert (
            key
            == COMPLETION_OBJECT_KEY
        )

        return encode_event(
            self._event
        )


class FailFirstPublishQueue:
    def __init__(
        self,
        *,
        delegate: LocalEventBus,
    ) -> None:
        self._delegate = delegate
        self.attempts = 0

    def publish(
        self,
        event: EventEnvelope,
    ) -> None:
        self.attempts += 1

        if self.attempts == 1:
            raise RuntimeError(
                "simulated Pub/Sub "
                "publish failure"
            )

        self._delegate.publish(
            event
        )


def completion_event(
) -> EventEnvelope:
    return EventEnvelope(
        event_id=(
            COMPLETION_EVENT_ID
        ),
        event_type=(
            EventType.PROCESSING_COMPLETED
        ),
        occurred_at=(
            COMPLETED_AT
        ),
        correlation_id=(
            CORRELATION_ID
        ),
        data={
            "provider": "bipad",
            "endpoint": (
                "river-stations"
            ),
            "run_id": "run-123",
            "manifest_path": (
                "manifests/provider=bipad/"
                "endpoint=river-stations/"
                "run_id=run-123/"
                "manifest.json"
            ),
            "quality_report_path": (
                QUALITY_REPORT_KEY
            ),
            "station_output_path": (
                "processed/"
                "dataset=stations/"
                "run_id=run-123"
            ),
            "observation_output_path": (
                "processed/"
                "dataset=observations/"
                "run_id=run-123"
            ),
            "ingestion_event_id": str(
                INGESTION_EVENT_ID
            ),
            "request_event_id": str(
                REQUEST_EVENT_ID
            ),
        },
    )


def gcs_push_payload(
) -> dict[str, object]:
    return {
        "message": {
            "attributes": {
                "eventType": (
                    "OBJECT_FINALIZE"
                ),
                "bucketId": (
                    BUCKET_NAME
                ),
                "objectId": (
                    COMPLETION_OBJECT_KEY
                ),
                "objectGeneration": (
                    "1"
                ),
            }
        }
    }


def test_relay_recovers_from_publish_failure_and_handles_redelivery(
    tmp_path: Path,
) -> None:
    event = (
        completion_event()
    )

    store: CompletionReceiptStore = (
        InMemoryReceiptStore(
            event=event,
        )
    )

    queue = LocalEventBus(
        events_root=(
            tmp_path
            / "events"
        ),
    )

    publisher = (
        FailFirstPublishQueue(
            delegate=queue,
        )
    )

    app = create_app(
        store=store,
        publisher=publisher,
    )

    client = TestClient(
        app
    )

    #
    # First delivery:
    #
    # receipt is valid, but publication
    # fails. The relay must return 503
    # so Pub/Sub will redeliver.
    #
    first_response = client.post(
        "/events/pubsub",
        json=gcs_push_payload(),
    )

    assert (
        first_response.status_code
        == 503
    )

    assert (
        publisher.attempts
        == 1
    )

    assert (
        queue.list_pending()
        == []
    )

    #
    # Second delivery:
    #
    # same GCS notification is retried.
    # The same durable event is read and
    # publication now succeeds.
    #
    second_response = client.post(
        "/events/pubsub",
        json=gcs_push_payload(),
    )

    assert (
        second_response.status_code
        == 204
    )

    assert (
        publisher.attempts
        == 2
    )

    pending = (
        queue.list_pending()
    )

    assert pending == [
        event
    ]

    assert (
        pending[0].event_id
        == COMPLETION_EVENT_ID
    )

    assert (
        pending[0].occurred_at
        == COMPLETED_AT
    )

    #
    # Third delivery simulates the case
    # where publication succeeded but the
    # HTTP acknowledgement was lost.
    #
    # The durable receipt contains exactly
    # the same event ID. LocalEventBus
    # therefore treats the duplicate
    # publication idempotently.
    #
    third_response = client.post(
        "/events/pubsub",
        json=gcs_push_payload(),
    )

    assert (
        third_response.status_code
        == 204
    )

    assert (
        publisher.attempts
        == 3
    )

    assert (
        queue.list_pending()
        == [event]
    )