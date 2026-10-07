from __future__ import annotations

from datetime import (
    UTC,
    datetime,
)
from typing import cast
from uuid import UUID

import pytest
from fastapi.testclient import (
    TestClient,
)

from riverwatch.cloud import (
    processing_completion_relay_app as relay_app,
)
from riverwatch.cloud.config import (
    ProcessingCompletionRelayCloudConfig,
)
from riverwatch.cloud.processing_completion_relay import (
    CompletionReceiptStore,
    ProcessingCompletionRelayError,
)
from riverwatch.events.bus import (
    EventPublisher,
)
from riverwatch.events.model import (
    EventEnvelope,
    EventType,
)

COMPLETION_EVENT_ID = UUID(
    "11111111-1111-1111-1111-111111111111"
)

CORRELATION_ID = UUID(
    "22222222-2222-2222-2222-222222222222"
)

COMPLETED_AT = datetime(
    2026,
    10,
    7,
    12,
    0,
    tzinfo=UTC,
)

COMPLETION_OBJECT_KEY = (
    "processed/provider=bipad/"
    "endpoint=river-stations/"
    "run_id=run-123/"
    "processing-completed.json"
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


class FailingPublisher:
    def __init__(
        self,
    ) -> None:
        self.attempts = 0

    def publish(
        self,
        event: EventEnvelope,
    ) -> None:
        _ = event

        self.attempts += 1

        raise RuntimeError(
            "publish failed"
        )


def completion_event(
) -> EventEnvelope:
    return EventEnvelope(
        event_id=COMPLETION_EVENT_ID,
        event_type=(
            EventType.PROCESSING_COMPLETED
        ),
        occurred_at=COMPLETED_AT,
        correlation_id=CORRELATION_ID,
        data={
            "provider": "bipad",
            "endpoint": "river-stations",
            "run_id": "run-123",
            "manifest_path": (
                "manifests/provider=bipad/"
                "endpoint=river-stations/"
                "run_id=run-123/"
                "manifest.json"
            ),
            "quality_report_path": (
                "quality/provider=bipad/"
                "endpoint=river-stations/"
                "run_id=run-123/"
                "report.json"
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
            "ingestion_event_id": (
                "33333333-3333-3333-3333-333333333333"
            ),
            "request_event_id": (
                "44444444-4444-4444-4444-444444444444"
            ),
        },
    )


def gcs_push_payload(
    *,
    object_id: str = COMPLETION_OBJECT_KEY,
) -> dict[str, object]:
    return {
        "message": {
            "attributes": {
                "eventType": (
                    "OBJECT_FINALIZE"
                ),
                "bucketId": (
                    "riverwatch-test-lake"
                ),
                "objectId": (
                    object_id
                ),
                "objectGeneration": (
                    "123456789"
                ),
            }
        }
    }


def test_health_live(
) -> None:
    app = relay_app.create_app(
        store=cast(
            CompletionReceiptStore,
            object(),
        ),
        publisher=RecordingPublisher(),
    )

    client = TestClient(
        app
    )

    response = client.get(
        "/health/live"
    )

    assert response.status_code == 200

    assert response.json() == {
        "status": "ok",
    }


def test_valid_completion_receipt_returns_204_and_publishes_event(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    expected_event = (
        completion_event()
    )

    expected_notification = (
        object()
    )

    expected_store = cast(
        CompletionReceiptStore,
        object(),
    )

    publisher = (
        RecordingPublisher()
    )

    def fake_decode(
        payload: dict[str, object],
    ) -> object:
        assert payload == (
            gcs_push_payload()
        )

        return (
            expected_notification
        )

    def fake_relay(
        *,
        notification: object,
        store: CompletionReceiptStore,
        publisher: EventPublisher,
    ) -> EventEnvelope:
        assert (
            notification
            is expected_notification
        )

        assert (
            store
            is expected_store
        )

        publisher.publish(
            expected_event
        )

        return expected_event

    monkeypatch.setattr(
        relay_app,
        "decode_gcs_finalize_push",
        fake_decode,
    )

    monkeypatch.setattr(
        relay_app,
        "relay_processing_completion",
        fake_relay,
    )

    app = relay_app.create_app(
        store=expected_store,
        publisher=publisher,
    )

    client = TestClient(
        app
    )

    response = client.post(
        "/events/pubsub",
        json=gcs_push_payload(),
    )

    assert (
        response.status_code
        == 204
    )

    assert publisher.events == [
        expected_event
    ]

    assert len(
        publisher.events
    ) == 1

    assert (
        publisher.events[0]
        .event_type
        is EventType.PROCESSING_COMPLETED
    )


def test_irrelevant_storage_object_returns_204_without_publishing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    expected_notification = (
        object()
    )

    publisher = (
        RecordingPublisher()
    )

    def fake_decode(
        payload: dict[str, object],
    ) -> object:
        assert payload == (
            gcs_push_payload(
                object_id=(
                    "quality/provider=bipad/"
                    "endpoint=river-stations/"
                    "run_id=run-123/"
                    "quality-report.json"
                )
            )
        )

        return (
            expected_notification
        )

    def fake_relay(
        *,
        notification: object,
        store: CompletionReceiptStore,
        publisher: EventPublisher,
    ) -> None:
        _ = (
            store,
            publisher,
        )

        assert (
            notification
            is expected_notification
        )

        return None

    monkeypatch.setattr(
        relay_app,
        "decode_gcs_finalize_push",
        fake_decode,
    )

    monkeypatch.setattr(
        relay_app,
        "relay_processing_completion",
        fake_relay,
    )

    app = relay_app.create_app(
        store=cast(
            CompletionReceiptStore,
            object(),
        ),
        publisher=publisher,
    )

    client = TestClient(
        app
    )

    response = client.post(
        "/events/pubsub",
        json=gcs_push_payload(
            object_id=(
                "quality/provider=bipad/"
                "endpoint=river-stations/"
                "run_id=run-123/"
                "quality-report.json"
            )
        ),
    )

    assert (
        response.status_code
        == 204
    )

    assert publisher.events == []


def test_malformed_notification_returns_400(
) -> None:
    app = relay_app.create_app(
        store=cast(
            CompletionReceiptStore,
            object(),
        ),
        publisher=RecordingPublisher(),
    )

    client = TestClient(
        app
    )

    response = client.post(
        "/events/pubsub",
        json={},
    )

    assert (
        response.status_code
        == 400
    )

def test_publish_failure_returns_503(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    expected_notification = (
        object()
    )

    expected_event = (
        completion_event()
    )

    publisher = (
        FailingPublisher()
    )

    def fake_decode(
        payload: dict[str, object],
    ) -> object:
        assert payload == (
            gcs_push_payload()
        )

        return (
            expected_notification
        )

    def fake_relay(
        *,
        notification: object,
        store: CompletionReceiptStore,
        publisher: EventPublisher,
    ) -> EventEnvelope:
        _ = store

        assert (
            notification
            is expected_notification
        )

        try:
            publisher.publish(
                expected_event
            )

        except Exception as exc:
            raise (
                ProcessingCompletionRelayError(
                    "Unable to publish "
                    "processing completion event"
                )
            ) from exc

        return expected_event

    monkeypatch.setattr(
        relay_app,
        "decode_gcs_finalize_push",
        fake_decode,
    )

    monkeypatch.setattr(
        relay_app,
        "relay_processing_completion",
        fake_relay,
    )

    app = relay_app.create_app(
        store=cast(
            CompletionReceiptStore,
            object(),
        ),
        publisher=publisher,
    )

    client = TestClient(
        app
    )

    response = client.post(
        "/events/pubsub",
        json=gcs_push_payload(),
    )

    assert (
        response.status_code
        == 503
    )

    assert publisher.attempts == 1

def test_builds_production_dependencies_from_config(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, str] = {}

    class FakeGcsObjectStore:
        def __init__(
            self,
            *,
            bucket_name: str,
        ) -> None:
            captured[
                "bucket_name"
            ] = bucket_name

    class FakePubSubEventPublisher:
        def __init__(
            self,
            *,
            topic: str,
        ) -> None:
            captured[
                "topic"
            ] = topic

    monkeypatch.setattr(
        relay_app,
        "GcsObjectStore",
        FakeGcsObjectStore,
    )

    monkeypatch.setattr(
        relay_app,
        "PubSubEventPublisher",
        FakePubSubEventPublisher,
    )

    config = (
        ProcessingCompletionRelayCloudConfig(
            lake_bucket=(
                "riverwatch-test-lake"
            ),
            processing_completed_topic=(
                "projects/test/topics/"
                "processing-completed"
            ),
        )
    )

    app = relay_app.create_app(
        config=config,
    )

    assert app.title == (
        "RiverWatch Processing "
        "Completion Relay"
    )

    assert captured == {
        "bucket_name": (
            "riverwatch-test-lake"
        ),
        "topic": (
            "projects/test/topics/"
            "processing-completed"
        ),
    }