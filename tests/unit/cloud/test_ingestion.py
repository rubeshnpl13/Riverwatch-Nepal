from __future__ import annotations

from datetime import (
    UTC,
    datetime,
)
from uuid import UUID

import pytest

from riverwatch.cloud.ingestion import (
    CloudIngestionHandler,
    ingestion_completion_event_id,
)
from riverwatch.events.model import (
    EventEnvelope,
    EventType,
)
from riverwatch.events.worker import (
    IngestionRunReference,
)

REQUEST_EVENT_ID = UUID(
    "11111111-1111-1111-1111-111111111111"
)

CORRELATION_ID = UUID(
    "22222222-2222-2222-2222-222222222222"
)

REQUEST_TIME = datetime(
    2026,
    10,
    5,
    0,
    0,
    tzinfo=UTC,
)

COMPLETED_TIME = datetime(
    2026,
    10,
    5,
    0,
    0,
    5,
    tzinfo=UTC,
)


class RecordingRunner:
    def __init__(
        self,
        result: IngestionRunReference,
    ) -> None:
        self.result = result

        self.calls: list[
            tuple[
                str,
                str,
                UUID,
            ]
        ] = []

    def run(
        self,
        *,
        provider: str,
        endpoint: str,
        idempotency_key: UUID,
    ) -> IngestionRunReference:
        self.calls.append(
            (
                provider,
                endpoint,
                idempotency_key,
            )
        )

        return self.result


class FailingRunner:
    def run(
        self,
        *,
        provider: str,
        endpoint: str,
        idempotency_key: UUID,
    ) -> IngestionRunReference:
        raise RuntimeError(
            "ingestion failed"
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
    def publish(
        self,
        event: EventEnvelope,
    ) -> None:
        raise RuntimeError(
            "publish failed"
        )


def make_request() -> EventEnvelope:
    return EventEnvelope(
        event_id=REQUEST_EVENT_ID,
        event_type=(
            EventType
            .INGESTION_REQUESTED
        ),
        occurred_at=REQUEST_TIME,
        correlation_id=(
            CORRELATION_ID
        ),
        data={
            "provider": "bipad",
            "endpoint": "river-stations",
        },
    )


def make_result() -> IngestionRunReference:
    return IngestionRunReference(
        run_id=(
            "event-"
            f"{REQUEST_EVENT_ID}"
            "-attempt-0001"
        ),
        manifest_path=(
            "manifests/provider=bipad/"
            "endpoint=river-stations/"
            "run_id=test/"
            "manifest.json"
        ),
        completed_at=(
            COMPLETED_TIME
        ),
    )


def test_handles_ingestion_request() -> None:
    runner = RecordingRunner(
        make_result()
    )

    publisher = RecordingPublisher()

    handler = CloudIngestionHandler(
        runner=runner,
        publisher=publisher,
    )

    completed = handler.handle(
        make_request()
    )

    assert runner.calls == [
        (
            "bipad",
            "river-stations",
            REQUEST_EVENT_ID,
        )
    ]

    assert len(
        publisher.events
    ) == 1

    assert (
        publisher.events[0]
        == completed
    )

    assert (
        completed.event_type
        is EventType.INGESTION_COMPLETED
    )

    assert (
        completed.correlation_id
        == CORRELATION_ID
    )

    assert (
        completed.occurred_at
        == COMPLETED_TIME
    )

    assert dict(
        completed.data
    ) == {
        "provider": "bipad",
        "endpoint": "river-stations",
        "run_id": (
            make_result().run_id
        ),
        "manifest_path": (
            make_result().manifest_path
        ),
        "request_event_id": str(
            REQUEST_EVENT_ID
        ),
    }


def test_completion_event_id_is_deterministic() -> None:
    first = ingestion_completion_event_id(
        REQUEST_EVENT_ID
    )

    second = ingestion_completion_event_id(
        REQUEST_EVENT_ID
    )

    assert first == second


def test_redelivery_produces_same_completion_event() -> None:
    runner = RecordingRunner(
        make_result()
    )

    publisher = RecordingPublisher()

    handler = CloudIngestionHandler(
        runner=runner,
        publisher=publisher,
    )

    request = make_request()

    first = handler.handle(
        request
    )

    second = handler.handle(
        request
    )

    assert first == second

    assert len(
        publisher.events
    ) == 2

    assert (
        publisher.events[0]
        == publisher.events[1]
    )


def test_uses_clock_when_runner_has_no_completion_time() -> None:
    result = IngestionRunReference(
        run_id="run-123",
        manifest_path="manifest.json",
    )

    handler = CloudIngestionHandler(
        runner=RecordingRunner(
            result
        ),
        publisher=(
            RecordingPublisher()
        ),
        clock=lambda: (
            COMPLETED_TIME
        ),
    )

    completed = handler.handle(
        make_request()
    )

    assert (
        completed.occurred_at
        == COMPLETED_TIME
    )


def test_rejects_non_ingestion_event() -> None:
    event = EventEnvelope(
        event_id=REQUEST_EVENT_ID,
        event_type=(
            EventType
            .INGESTION_COMPLETED
        ),
        occurred_at=REQUEST_TIME,
        correlation_id=(
            CORRELATION_ID
        ),
        data={
            "provider": "bipad",
            "endpoint": "river-stations",
        },
    )

    handler = CloudIngestionHandler(
        runner=RecordingRunner(
            make_result()
        ),
        publisher=(
            RecordingPublisher()
        ),
    )

    with pytest.raises(
        ValueError,
        match="ingestion.requested",
    ):
        handler.handle(
            event
        )


@pytest.mark.parametrize(
    (
        "data",
        "message",
    ),
    [
        (
            {
                "endpoint":
                    "river-stations",
            },
            "provider must be a string",
        ),
        (
            {
                "provider": "   ",
                "endpoint":
                    "river-stations",
            },
            "provider must not be blank",
        ),
        (
            {
                "provider": "bipad",
            },
            "endpoint must be a string",
        ),
        (
            {
                "provider": "bipad",
                "endpoint": "   ",
            },
            "endpoint must not be blank",
        ),
    ],
)
def test_rejects_invalid_request_data(
    data: dict[str, str],
    message: str,
) -> None:
    event = EventEnvelope(
        event_id=REQUEST_EVENT_ID,
        event_type=(
            EventType
            .INGESTION_REQUESTED
        ),
        occurred_at=REQUEST_TIME,
        correlation_id=(
            CORRELATION_ID
        ),
        data=data,
    )

    handler = CloudIngestionHandler(
        runner=RecordingRunner(
            make_result()
        ),
        publisher=(
            RecordingPublisher()
        ),
    )

    with pytest.raises(
        ValueError,
        match=message,
    ):
        handler.handle(
            event
        )


def test_runner_failure_propagates() -> None:
    handler = CloudIngestionHandler(
        runner=FailingRunner(),
        publisher=RecordingPublisher(),
    )

    with pytest.raises(
        RuntimeError,
        match="ingestion failed",
    ):
        handler.handle(
            make_request()
        )


def test_publish_failure_propagates() -> None:
    handler = CloudIngestionHandler(
        runner=RecordingRunner(
            make_result()
        ),
        publisher=FailingPublisher(),
    )

    with pytest.raises(
        RuntimeError,
        match="publish failed",
    ):
        handler.handle(
            make_request()
        )