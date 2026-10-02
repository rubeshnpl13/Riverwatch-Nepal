from datetime import (
    UTC,
    datetime,
)
from pathlib import Path
from uuid import UUID

import pytest

from riverwatch.events.idempotency import (
    IngestionExecutionRecord,
    LocalIngestionExecutionStore,
)
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

        return self._result


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
            "provider": "bipad",
            "endpoint": "river-stations",
        },
    )


def test_worker_processes_ingestion_request(
    tmp_path: Path,
) -> None:
    queue = LocalEventBus(
        events_root=tmp_path,
    )

    request = make_request_event()

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
        execution_store=(
            LocalIngestionExecutionStore(
                events_root=tmp_path,
            )
        ),
        clock=lambda: COMPLETED_TIME,
        uuid_factory=lambda: (
            COMPLETED_EVENT_ID
        ),
    )

    processed = worker.run_once()

    assert processed == 1

    assert runner.calls == [
        (
            "bipad",
            "river-stations",
            REQUEST_EVENT_ID,
        )
    ]


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
        execution_store=(
            LocalIngestionExecutionStore(
                events_root=tmp_path,
            )
        ),
        clock=lambda: COMPLETED_TIME,
        uuid_factory=lambda: (
            COMPLETED_EVENT_ID
        ),
    )

    worker.run_once()

    pending = queue.list_pending()

    assert len(pending) == 1

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
        dict(completed.data)
        == {
            "provider": "bipad",
            "endpoint": "river-stations",
            "run_id": "run-123",
            "manifest_path": (
                "manifests/"
                "run-123/"
                "manifest.json"
            ),
            "request_event_id": str(
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
        execution_store=(
            LocalIngestionExecutionStore(
                events_root=tmp_path,
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
        / f"{REQUEST_EVENT_ID}.json"
    )

    assert processed_path.is_file()


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
        execution_store=(
            LocalIngestionExecutionStore(
                events_root=tmp_path,
            )
        ),
    )

    with pytest.raises(
        RuntimeError,
        match="ingestion failed",
    ):
        worker.run_once()

    pending = queue.list_pending()

    assert len(pending) == 1

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
        event_id=COMPLETED_EVENT_ID,
        event_type=(
            EventType
            .INGESTION_COMPLETED
        ),
        occurred_at=COMPLETED_TIME,
        correlation_id=CORRELATION_ID,
        data={
            "provider": "bipad",
            "endpoint": "river-stations",
            "run_id": "run-123",
            "manifest_path": "manifest.json",
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
        execution_store=(
            LocalIngestionExecutionStore(
                events_root=tmp_path,
            )
        ),
    )

    assert worker.run_once() == 0

    assert runner.calls == []

    assert (
        queue.list_pending()
        == [completed]
    )


# Validation tests for the run reference


def test_run_reference_rejects_blank_run_id() -> None:
    with pytest.raises(
        ValueError,
        match=(
            "run_id must not be blank"
        ),
    ):
        IngestionRunReference(
            run_id="   ",
            manifest_path="manifest.json",
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


def test_worker_does_not_rerun_completed_request(
    tmp_path: Path,
) -> None:
    queue = LocalEventBus(
        events_root=tmp_path,
    )

    request = make_request_event()

    queue.publish(
        request
    )

    execution_store = (
        LocalIngestionExecutionStore(
            events_root=tmp_path,
        )
    )

    execution_store.save(
        IngestionExecutionRecord(
            request_event_id=REQUEST_EVENT_ID,
            completion_event_id=(
                COMPLETED_EVENT_ID
            ),
            correlation_id=CORRELATION_ID,
            provider="bipad",
            endpoint="river-stations",
            run_id="run-123",
            manifest_path="manifest.json",
            occurred_at=COMPLETED_TIME,
        )
    )

    runner = RecordingRunner(
        IngestionRunReference(
            run_id="should-not-run",
            manifest_path="should-not-run",
        )
    )

    worker = IngestionWorker(
        queue=queue,
        runner=runner,
        execution_store=execution_store,
    )

    assert worker.run_once() == 1

    assert runner.calls == []

    pending = queue.list_pending()

    assert len(pending) == 1

    assert (
        pending[0].event_id
        == COMPLETED_EVENT_ID
    )

    assert (
        pending[0].event_type
        == EventType
        .INGESTION_COMPLETED
    )

    processed_path = (
        tmp_path
        / "processed"
        / f"{REQUEST_EVENT_ID}.json"
    )

    assert processed_path.is_file()


class FailCompletionPublishOnceQueue:
    def __init__(
        self,
        delegate: LocalEventBus,
    ) -> None:
        self._delegate = delegate
        self._failed = False

    def publish(
        self,
        event: EventEnvelope,
    ) -> None:
        if (
            event.event_type
            == EventType
            .INGESTION_COMPLETED
            and not self._failed
        ):
            self._failed = True

            raise RuntimeError(
                "completion publish failed"
            )

        self._delegate.publish(
            event
        )

    def list_pending(
        self,
    ) -> list[EventEnvelope]:
        return (
            self._delegate
            .list_pending()
        )

    def mark_processed(
        self,
        event_id: UUID,
    ) -> None:
        self._delegate.mark_processed(
            event_id
        )


def test_worker_recovers_after_completion_publish_failure(
    tmp_path: Path,
) -> None:
    local_queue = LocalEventBus(
        events_root=tmp_path,
    )

    local_queue.publish(
        make_request_event()
    )

    queue = (
        FailCompletionPublishOnceQueue(
            local_queue
        )
    )

    execution_store = (
        LocalIngestionExecutionStore(
            events_root=tmp_path,
        )
    )

    runner = RecordingRunner(
        IngestionRunReference(
            run_id="run-123",
            manifest_path="manifest.json",
        )
    )

    worker = IngestionWorker(
        queue=queue,
        runner=runner,
        execution_store=execution_store,
        clock=lambda: COMPLETED_TIME,
        uuid_factory=lambda: (
            COMPLETED_EVENT_ID
        ),
    )

    with pytest.raises(
        RuntimeError,
        match=(
            "completion publish failed"
        ),
    ):
        worker.run_once()

    assert runner.calls == [
        (
            "bipad",
            "river-stations",
            REQUEST_EVENT_ID,
        )
    ]

    assert (
        execution_store.get(
            REQUEST_EVENT_ID
        )
        is not None
    )

    assert (
        local_queue.list_pending()[0]
        .event_id
        == REQUEST_EVENT_ID
    )

    assert worker.run_once() == 1

    # Critically, ingestion was NOT
    # executed a second time.
    assert runner.calls == [
        (
            "bipad",
            "river-stations",
            REQUEST_EVENT_ID,
        )
    ]

    pending = local_queue.list_pending()

    assert len(pending) == 1

    assert (
        pending[0].event_id
        == COMPLETED_EVENT_ID
    )

    assert (
        pending[0].event_type
        == EventType
        .INGESTION_COMPLETED
    )