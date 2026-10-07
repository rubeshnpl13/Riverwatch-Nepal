import base64
from datetime import (
    UTC,
    datetime,
)
from uuid import UUID

from fastapi.testclient import (
    TestClient,
)

from riverwatch.cloud.dataproc import (
    DataprocBatchSubmission,
)
from riverwatch.cloud.processing_app import (
    create_app,
)
from riverwatch.cloud.processing_gateway import (
    ProcessingGatewayHandler,
)
from riverwatch.events.model import (
    EventEnvelope,
    EventType,
)
from riverwatch.events.serialization import (
    encode_event,
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
    ) -> DataprocBatchSubmission:
        self.events.append(
            event
        )

        return DataprocBatchSubmission(
            batch_id=(
                "rw-processing-test"
            ),
            resource_name=(
                "projects/test/"
                "locations/asia-south1/"
                "batches/"
                "rw-processing-test"
            ),
        )


class FailingHandler:
    def handle(
        self,
        event: EventEnvelope,
    ) -> DataprocBatchSubmission:
        raise RuntimeError(
            "Dataproc submission failed"
        )


def make_event() -> EventEnvelope:

    return EventEnvelope(
        event_id=UUID(
            "11111111-1111-1111-1111-111111111111"
        ),
        event_type=(
            EventType
            .INGESTION_COMPLETED
        ),
        occurred_at=datetime(
            2026,
            10,
            5,
            0,
            0,
            tzinfo=UTC,
        ),
        correlation_id=UUID(
            "22222222-2222-2222-2222-222222222222"
        ),
        data={
            "provider": "bipad",
            "endpoint": "river-stations",
            "run_id": "run-123",
            "manifest_path": (
                "manifests/run-123/"
                "manifest.json"
            ),
            "request_event_id": (
                "33333333-3333-3333-"
                "3333-333333333333"
            ),
        },
    )


def push_payload(
    event: EventEnvelope,
) -> dict[str, object]:
    encoded = base64.b64encode(
        encode_event(
            event
        )
    ).decode(
        "ascii"
    )

    return {
        "message": {
            "data": encoded,
            "messageId": "987654321",
            "publishTime": (
                "2026-10-05T00:01:00Z"
            ),
        },
        "subscription": (
            "projects/test/"
            "subscriptions/"
            "riverwatch-dev-"
            "processing-worker"
        ),
    }


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


def test_accepts_ingestion_completed_event() -> None:
    handler = RecordingHandler()

    app = create_app(
        handler=handler
    )

    event = make_event()

    with TestClient(
        app
    ) as client:
        response = client.post(
            "/events/pubsub",
            json=push_payload(
                event
            ),
        )

    assert (
        response.status_code
        == 204
    )

    assert response.content == b""

    assert (
        handler.events
        == [event]
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
                    "projects/test/"
                    "subscriptions/"
                    "processing-worker"
                ),
            },
        )

    assert (
        response.status_code
        == 400
    )

    assert handler.events == []


def test_rejects_non_object_body() -> None:
    app = create_app(
        handler=RecordingHandler()
    )

    with TestClient(
        app
    ) as client:
        response = client.post(
            "/events/pubsub",
            json=[
                "invalid",
                "body",
            ],
        )

    assert (
        response.status_code
        == 422
    )


def test_submission_failure_returns_500() -> None:
    app = create_app(
        handler=FailingHandler()
    )

    with TestClient(
        app,
        raise_server_exceptions=False,
    ) as client:
        response = client.post(
            "/events/pubsub",
            json=push_payload(
                make_event()
            ),
        )

    assert (
        response.status_code
        == 500
    )


def test_wrong_event_type_returns_500() -> None:
    handler = (
        build_real_gateway_handler_for_test()
    )

    event = make_event()

    wrong_event = EventEnvelope(
        event_id=event.event_id,
        event_type=(
            EventType
            .INGESTION_REQUESTED
        ),
        occurred_at=(
            event.occurred_at
        ),
        correlation_id=(
            event.correlation_id
        ),
        data={
            "provider": "bipad",
            "endpoint": "river-stations",
        },
    )

    app = create_app(
        handler=handler
    )

    with TestClient(
        app,
        raise_server_exceptions=False,
    ) as client:
        response = client.post(
            "/events/pubsub",
            json=push_payload(
                wrong_event
            ),
        )

    assert (
        response.status_code
        == 500
    )


def build_real_gateway_handler_for_test(
) -> ProcessingGatewayHandler:

    class NeverCalledSubmitter:
        def submit(
            self,
            event: EventEnvelope,
        ) -> DataprocBatchSubmission:
            raise AssertionError(
                "submitter must not be called"
            )

    return ProcessingGatewayHandler(
        submitter=(
            NeverCalledSubmitter()
        )
    )