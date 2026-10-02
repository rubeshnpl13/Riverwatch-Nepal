from datetime import (
    UTC,
    datetime,
)
from pathlib import Path
from uuid import UUID

import pytest

from riverwatch.events.local import (
    LocalEventBus,
)
from riverwatch.events.model import (
    EventEnvelope,
    EventType,
)
from riverwatch.events.worker import (
    IngestionRunReference,
    IngestionWorker,
)

REQUEST_EVENT_ID = UUID(
    "11111111-1111-1111-1111-111111111111"
)

COMPLETED_EVENT_ID = UUID(
    "22222222-2222-2222-2222-222222222222"
)

CORRELATION_ID = UUID(
    "33333333-3333-3333-3333-333333333333"
)

REQUEST_TIME = datetime(
    2026,
    10,
    2,
    0,
    0,
    tzinfo=UTC,
)

COMPLETED_TIME = datetime(
    2026,
    10,
    2,
    0,
    1,
    tzinfo=UTC,
)


class RecordingRunner:
    def __init__(
        self,
        result: IngestionRunReference,
    ) -> None:
        self._result = result

        self.calls: list[
            tuple[str, str]
        ] = []

    def run(
        self,
        *,
        provider: str,
        endpoint: str,
    ) -> IngestionRunReference:
        self.calls.append(
            (
                provider,
                endpoint,
            )
        )

        return self._result


class FailingRunner:
    def run(
        self,
        *,
        provider: str,
        endpoint: str,
    ) -> IngestionRunReference:
        raise RuntimeError(
            "ingestion failed"
        )


def make_request_event() -> EventEnvelope:
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
            "provider":
                "bipad",

            "endpoint":
                "river-stations",
        },
    )


def test_worker_processes_ingestion_request(
    tmp_path: Path,
) -> None:
    queue = LocalEventBus(
        events_root=tmp_path,
    )

    request = (
        make_request_event()
    )

    queue.publish(
        request
    )

    runner = RecordingRunner(
        IngestionRunReference(
            run_id="run-123",
            manifest_path=(
                "manifests/provider=bipad/"
                "run-123/manifest.json"
            ),
        )
    )

    worker = IngestionWorker(
        queue=queue,
        runner=runner,
        clock=lambda: (
            COMPLETED_TIME
        ),
        uuid_factory=lambda: (
            COMPLETED_EVENT_ID
        ),
    )

    processed = (
        worker.run_once()
    )

    assert processed == 1

    assert (
        runner.calls
        == [
            (
                "bipad",
                "river-stations",
            )
        ]
    )


def test_worker_publishes_completed_event(
    tmp_path: Path,
) -> None:
    queue = LocalEventBus(
        events_root=tmp_path,
    )

    queue.publish(
        make_request_event()
    )

    worker = IngestionWorker(
        queue=queue,
        runner=RecordingRunner(
            IngestionRunReference(
                run_id="run-123",
                manifest_path=(
                    "manifests/"
                    "run-123/"
                    "manifest.json"
                ),
            )
        ),
        clock=lambda: (
            COMPLETED_TIME
        ),
        uuid_factory=lambda: (
            COMPLETED_EVENT_ID
        ),
    )

    worker.run_once()

    pending = (
        queue.list_pending()
    )

    assert len(
        pending
    ) == 1

    completed = pending[0]

    assert (
        completed.event_id
        == COMPLETED_EVENT_ID
    )

    assert (
        completed.event_type
        == EventType
        .INGESTION_COMPLETED
    )

    assert (
        completed.correlation_id
        == CORRELATION_ID
    )

    assert (
        completed.occurred_at
        == COMPLETED_TIME
    )

    assert (
        dict(
            completed.data
        )
        == {
            "provider":
                "bipad",

            "endpoint":
                "river-stations",

            "run_id":
                "run-123",

            "manifest_path":
                (
                    "manifests/"
                    "run-123/"
                    "manifest.json"
                ),

            "request_event_id":
                str(
                    REQUEST_EVENT_ID
                ),
        }
    )


def test_worker_marks_request_processed(
    tmp_path: Path,
) -> None:
    queue = LocalEventBus(
        events_root=tmp_path,
    )

    queue.publish(
        make_request_event()
    )

    worker = IngestionWorker(
        queue=queue,
        runner=RecordingRunner(
            IngestionRunReference(
                run_id="run-123",
                manifest_path=(
                    "manifest.json"
                ),
            )
        ),
        uuid_factory=lambda: (
            COMPLETED_EVENT_ID
        ),
    )

    worker.run_once()

    processed_path = (
        tmp_path
        / "processed"
        / (
            f"{REQUEST_EVENT_ID}"
            ".json"
        )
    )

    assert (
        processed_path.is_file()
    )


def test_worker_leaves_request_pending_when_runner_fails(
    tmp_path: Path,
) -> None:
    queue = LocalEventBus(
        events_root=tmp_path,
    )

    queue.publish(
        make_request_event()
    )

    worker = IngestionWorker(
        queue=queue,
        runner=FailingRunner(),
    )

    with pytest.raises(
        RuntimeError,
        match="ingestion failed",
    ):
        worker.run_once()

    pending = (
        queue.list_pending()
    )

    assert len(
        pending
    ) == 1

    assert (
        pending[0].event_id
        == REQUEST_EVENT_ID
    )


def test_worker_ignores_non_request_events(
    tmp_path: Path,
) -> None:
    queue = LocalEventBus(
        events_root=tmp_path,
    )

    completed = EventEnvelope(
        event_id=(
            COMPLETED_EVENT_ID
        ),
        event_type=(
            EventType
            .INGESTION_COMPLETED
        ),
        occurred_at=(
            COMPLETED_TIME
        ),
        correlation_id=(
            CORRELATION_ID
        ),
        data={
            "provider":
                "bipad",

            "endpoint":
                "river-stations",

            "run_id":
                "run-123",

            "manifest_path":
                "manifest.json",
        },
    )

    queue.publish(
        completed
    )

    runner = RecordingRunner(
        IngestionRunReference(
            run_id="unused",
            manifest_path="unused",
        )
    )

    worker = IngestionWorker(
        queue=queue,
        runner=runner,
    )

    assert (
        worker.run_once()
        == 0
    )

    assert runner.calls == []

    assert (
        queue.list_pending()
        == [
            completed
        ]
    )

#validation tests for the run reference
def test_run_reference_rejects_blank_run_id() -> None:
    with pytest.raises(
        ValueError,
        match=(
            "run_id must not be blank"
        ),
    ):
        IngestionRunReference(
            run_id="   ",
            manifest_path=(
                "manifest.json"
            ),
        )


def test_run_reference_rejects_blank_manifest_path() -> None:
    with pytest.raises(
        ValueError,
        match=(
            "manifest_path must not be blank"
        ),
    ):
        IngestionRunReference(
            run_id="run-123",
            manifest_path="   ",
        )