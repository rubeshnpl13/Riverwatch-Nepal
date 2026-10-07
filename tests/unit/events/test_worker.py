import json
import logging
from datetime import (
    UTC,
    datetime,
)
from io import StringIO
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
from riverwatch.events.retry import (
    LocalIngestionRetryStore,
    RetryPolicy,
)
from riverwatch.events.worker import (
    IngestionRunReference,
    IngestionWorker,
)
from riverwatch.observability.logging import (
    configure_logging,
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
        correlation_id=CORRELATION_ID,
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
        retry_store=(
            LocalIngestionRetryStore(
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
        retry_store=(
            LocalIngestionRetryStore(
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
                manifest_path="manifest.json",
            )
        ),
        execution_store=(
            LocalIngestionExecutionStore(
                events_root=tmp_path,
            )
        ),
        retry_store=(
            LocalIngestionRetryStore(
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


def test_worker_records_retry_when_runner_fails(
    tmp_path: Path,
) -> None:
    queue = LocalEventBus(
        events_root=tmp_path,
    )

    queue.publish(
        make_request_event()
    )

    retry_store = (
        LocalIngestionRetryStore(
            events_root=tmp_path,
        )
    )

    worker = IngestionWorker(
        queue=queue,
        runner=FailingRunner(),
        execution_store=(
            LocalIngestionExecutionStore(
                events_root=tmp_path,
            )
        ),
        retry_store=retry_store,
        clock=lambda: COMPLETED_TIME,
    )

    assert worker.run_once() == 0

    pending = queue.list_pending()

    assert len(pending) == 1

    assert (
        pending[0].event_id
        == REQUEST_EVENT_ID
    )

    state = retry_store.get(
        REQUEST_EVENT_ID
    )

    assert state is not None

    assert state.attempts == 1

    assert (
        state.last_error_type
        == "RuntimeError"
    )

    assert (
        state.last_error_message
        == "ingestion failed"
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
        retry_store=(
            LocalIngestionRetryStore(
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
        retry_store=(
            LocalIngestionRetryStore(
                events_root=tmp_path,
            )
        ),
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

    def mark_failed(
        self,
        event_id: UUID,
    ) -> None:
        self._delegate.mark_failed(
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

    retry_store = (
        LocalIngestionRetryStore(
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
        retry_store=retry_store,
        clock=lambda: COMPLETED_TIME,
        uuid_factory=lambda: (
            COMPLETED_EVENT_ID
        ),
    )

    assert worker.run_once() == 0

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

    retry_state = retry_store.get(
        REQUEST_EVENT_ID
    )

    assert retry_state is not None

    assert retry_state.attempts == 1

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

    assert (
        retry_store.get(
            REQUEST_EVENT_ID
        )
        is None
    )


# Add dead-letter test


def test_worker_dead_letters_request_after_max_attempts(
    tmp_path: Path,
) -> None:
    queue = LocalEventBus(
        events_root=tmp_path,
    )

    queue.publish(
        make_request_event()
    )

    retry_store = (
        LocalIngestionRetryStore(
            events_root=tmp_path,
        )
    )

    worker = IngestionWorker(
        queue=queue,
        runner=FailingRunner(),
        execution_store=(
            LocalIngestionExecutionStore(
                events_root=tmp_path,
            )
        ),
        retry_store=retry_store,
        retry_policy=(
            RetryPolicy(
                max_attempts=3,
            )
        ),
        clock=lambda: (
            COMPLETED_TIME
        ),
    )

    assert worker.run_once() == 0
    assert worker.run_once() == 0
    assert worker.run_once() == 0

    assert (
        queue.list_pending()
        == []
    )

    failed_path = (
        tmp_path
        / "failed"
        / (
            f"{REQUEST_EVENT_ID}"
            ".json"
        )
    )

    assert failed_path.is_file()

    state = retry_store.get(
        REQUEST_EVENT_ID
    )

    assert state is not None

    assert state.attempts == 3

    assert (
        state.last_error_type
        == "RuntimeError"
    )

    assert (
        state.last_error_message
        == "ingestion failed"
    )


# Success-clears-retries regression test


def test_success_clears_previous_retry_state(
    tmp_path: Path,
) -> None:
    queue = LocalEventBus(
        events_root=tmp_path,
    )

    queue.publish(
        make_request_event()
    )

    retry_store = (
        LocalIngestionRetryStore(
            events_root=tmp_path,
        )
    )

    retry_store.record_failure(
        request_event_id=REQUEST_EVENT_ID,
        failed_at=REQUEST_TIME,
        error_type="RuntimeError",
        error_message="previous failure",
    )

    worker = IngestionWorker(
        queue=queue,
        runner=RecordingRunner(
            IngestionRunReference(
                run_id="run-123",
                manifest_path="manifest.json",
            )
        ),
        execution_store=(
            LocalIngestionExecutionStore(
                events_root=tmp_path,
            )
        ),
        retry_store=retry_store,
        clock=lambda: (
            COMPLETED_TIME
        ),
        uuid_factory=lambda: (
            COMPLETED_EVENT_ID
        ),
    )

    assert (
        worker.run_once()
        == 1
    )

    assert (
        retry_store.get(
            REQUEST_EVENT_ID
        )
        is None
    )

def test_worker_logs_successful_ingestion_lifecycle(
    tmp_path: Path,
) -> None:
    output = StringIO()

    configure_logging(
        stream=output
    )

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
        retry_store=(
            LocalIngestionRetryStore(
                events_root=tmp_path,
            )
        ),
        clock=lambda: COMPLETED_TIME,
        uuid_factory=lambda: (
            COMPLETED_EVENT_ID
        ),
        logger=logging.getLogger(
            "riverwatch.test.ingestion"
        ),
    )

    assert worker.run_once() == 1

    documents = [
        json.loads(line)
        for line in (
            output
            .getvalue()
            .splitlines()
        )
        if line
    ]

    events = [
        document["event"]
        for document in documents
    ]

    assert events == [
        "ingestion_request_started",
        "ingestion_request_completed",
    ]

    completed = documents[-1]

    assert (
        completed["event_id"]
        == str(REQUEST_EVENT_ID)
    )

    assert (
        completed["correlation_id"]
        == str(CORRELATION_ID)
    )

    assert (
        completed["run_id"]
        == "run-123"
    )

def test_run_reference_rejects_naive_completed_at() -> None:
    with pytest.raises(
        ValueError,
        match="completed_at must be timezone-aware",
    ):
        IngestionRunReference(
            run_id="run-123",
            manifest_path="manifest.json",
            completed_at=datetime(
                2026,
                10,
                5,
                0,
                0,
            ),
        )