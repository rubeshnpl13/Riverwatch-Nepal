from __future__ import annotations

import base64
import json
from uuid import UUID

from fastapi.testclient import (
    TestClient,
)

from riverwatch.cloud.ingestion_app import (
    create_app,
)
from riverwatch.events.model import (
    EventEnvelope,
    EventType,
)

REQUEST_EVENT_ID = UUID(
    "11111111-1111-1111-1111-111111111111"
)

CORRELATION_ID = UUID(
    "22222222-2222-2222-2222-222222222222"
)

SUBSCRIPTION = (
    "projects/test-project/"
    "subscriptions/"
    "riverwatch-dev-ingestion-worker"
)


class RecordingHandler:
    def __init__(
        self,
    ) -> None:
        self.events: list[
            EventEnvelope
        ] = []

    def handle(
        self,
        event: EventEnvelope,
    ) -> EventEnvelope:
        self.events.append(
            event
        )

        return event


class FailingHandler:
    def handle(
        self,
        event: EventEnvelope,
    ) -> EventEnvelope:
        raise RuntimeError(
            "ingestion failed"
        )


def _push_payload(
    data: bytes,
) -> dict[str, object]:
    return {
        "message": {
            "data": (
                base64
                .b64encode(
                    data
                )
                .decode(
                    "ascii"
                )
            ),
            "messageId": "123456789",
            "publishTime": (
                "2026-10-05T00:15:00Z"
            ),
        },
        "subscription": (
            SUBSCRIPTION
        ),
    }


def _scheduled_trigger(
) -> bytes:
    return json.dumps(
        {
            "schema_version": 1,
            "message_kind": (
                "scheduled_ingestion_trigger"
            ),
            "provider": "bipad",
            "endpoint": (
                "river-stations"
            ),
        }
    ).encode(
        "utf-8"
    )


def _riverwatch_request(
) -> bytes:
    payload = {
        "event_id": str(
            REQUEST_EVENT_ID
        ),
        "event_type": (
            "ingestion.requested"
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
            "endpoint": (
                "river-stations"
            ),
        },
    }

    return json.dumps(
        payload
    ).encode(
        "utf-8"
    )


def test_health_live() -> None:
    app = create_app(
        handler=RecordingHandler()
    )

    with TestClient(
        app
    ) as client:
        response = client.get(
            "/health/live"
        )

    assert (
        response.status_code
        == 200
    )

    assert response.json() == {
        "status": "ok",
    }


def test_accepts_scheduled_trigger() -> None:
    handler = RecordingHandler()

    app = create_app(
        handler=handler
    )

    with TestClient(
        app
    ) as client:
        response = client.post(
            "/events/pubsub",
            json=_push_payload(
                _scheduled_trigger()
            ),
        )

    assert (
        response.status_code
        == 204
    )

    assert response.content == b""

    assert len(
        handler.events
    ) == 1

    event = handler.events[0]

    assert (
        event.event_type
        is EventType
        .INGESTION_REQUESTED
    )

    assert dict(
        event.data
    ) == {
        "provider": "bipad",
        "endpoint": (
            "river-stations"
        ),
    }


def test_accepts_riverwatch_request() -> None:
    handler = RecordingHandler()

    app = create_app(
        handler=handler
    )

    with TestClient(
        app
    ) as client:
        response = client.post(
            "/events/pubsub",
            json=_push_payload(
                _riverwatch_request()
            ),
        )

    assert (
        response.status_code
        == 204
    )

    assert len(
        handler.events
    ) == 1

    event = handler.events[0]

    assert (
        event.event_id
        == REQUEST_EVENT_ID
    )

    assert (
        event.correlation_id
        == CORRELATION_ID
    )


def test_rejects_invalid_pubsub_payload() -> None:
    handler = RecordingHandler()

    app = create_app(
        handler=handler
    )

    with TestClient(
        app
    ) as client:
        response = client.post(
            "/events/pubsub",
            json={
                "message": {
                    "data": "%%%",
                    "messageId": "123",
                    "publishTime": (
                        "2026-10-05T00:00:00Z"
                    ),
                },
                "subscription": (
                    SUBSCRIPTION
                ),
            },
        )

    assert (
        response.status_code
        == 400
    )

    assert handler.events == []


def test_rejects_non_object_http_body() -> None:
    app = create_app(
        handler=RecordingHandler()
    )

    with TestClient(
        app
    ) as client:
        response = client.post(
            "/events/pubsub",
            json=[
                "not",
                "an",
                "object",
            ],
        )

    assert (
        response.status_code
        == 422
    )


def test_handler_failure_returns_500() -> None:
    app = create_app(
        handler=FailingHandler()
    )

    with TestClient(
        app,
        raise_server_exceptions=False,
    ) as client:
        response = client.post(
            "/events/pubsub",
            json=_push_payload(
                _scheduled_trigger()
            ),
        )

    assert (
        response.status_code
        == 500
    )