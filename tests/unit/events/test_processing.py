import json
import logging
from datetime import (
    UTC,
    datetime,
)
from io import StringIO
from pathlib import Path
from uuid import UUID

from riverwatch.events.local import (
    LocalEventBus,
)
from riverwatch.events.model import (
    EventEnvelope,
    EventType,
)
from riverwatch.events.processing import (
    ProcessingRunReference,
    ProcessingWorker,
)
from riverwatch.events.processing_idempotency import (
    LocalProcessingExecutionStore,
    ProcessingExecutionRecord,
)
from riverwatch.events.processing_retry import (
    LocalProcessingRetryStore,
)
from riverwatch.events.retry import (
    RetryPolicy,
)
from riverwatch.observability.logging import (
    configure_logging,
)

INGESTION_EVENT_ID = UUID(
    "11111111-1111-1111-1111-111111111111"
)

PROCESSING_EVENT_ID = UUID(
    "22222222-2222-2222-2222-222222222222"
)

CORRELATION_ID = UUID(
    "33333333-3333-3333-3333-333333333333"
)

REQUEST_EVENT_ID = UUID(
    "44444444-4444-4444-4444-444444444444"
)

INGESTION_TIME = datetime(
    2026,
    10,
    3,
    0,
    0,
    tzinfo=UTC,
)

PROCESSING_TIME = datetime(
    2026,
    10,
    3,
    0,
    1,
    tzinfo=UTC,
)


class RecordingProcessingRunner:
    def __init__(
        self,
        result: ProcessingRunReference,
    ) -> None:
        self._result = result

        self.calls: list[
            tuple[
                str,
                str,
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
        run_id: str,
        manifest_path: str,
        idempotency_key: UUID,
    ) -> ProcessingRunReference:
        self.calls.append(
            (
                provider,
                endpoint,
                run_id,
                manifest_path,
                idempotency_key,
            )
        )

        return self._result


def make_ingestion_completed_event(
) -> EventEnvelope:
    return EventEnvelope(
        event_id=INGESTION_EVENT_ID,
        event_type=(
            EventType.INGESTION_COMPLETED
        ),
        occurred_at=INGESTION_TIME,
        correlation_id=CORRELATION_ID,
        data={
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
        },
    )


def test_processing_worker_runs_completed_ingestion(
    tmp_path: Path,
) -> None:
    queue = LocalEventBus(
        events_root=tmp_path,
    )

    queue.publish(
        make_ingestion_completed_event()
    )

    runner = (
        RecordingProcessingRunner(
            ProcessingRunReference(
                run_id="run-123",
                quality_report_path=(
                    "quality/run-123/"
                    "report.json"
                ),
                station_output_path=(
                    "processed/stations/"
                    "run-123"
                ),
                observation_output_path=(
                    "processed/observations/"
                    "run-123"
                ),
            )
        )
    )

    worker = ProcessingWorker(
        queue=queue,
        runner=runner,
        execution_store=(
            LocalProcessingExecutionStore(
                events_root=tmp_path,
            )
        ),
        retry_store=(
            LocalProcessingRetryStore(
                events_root=tmp_path,
            )
        ),
        clock=lambda: (
            PROCESSING_TIME
        ),
        uuid_factory=lambda: (
            PROCESSING_EVENT_ID
        ),
    )

    assert worker.run_once() == 1

    assert runner.calls == [
        (
            "bipad",
            "river-stations",
            "run-123",
            (
                "manifests/"
                "run-123/"
                "manifest.json"
            ),
            INGESTION_EVENT_ID,
        )
    ]


def test_processing_worker_publishes_completion(
    tmp_path: Path,
) -> None:
    queue = LocalEventBus(
        events_root=tmp_path,
    )

    queue.publish(
        make_ingestion_completed_event()
    )

    worker = ProcessingWorker(
        queue=queue,
        runner=(
            RecordingProcessingRunner(
                ProcessingRunReference(
                    run_id="run-123",
                    quality_report_path=(
                        "quality/run-123/"
                        "report.json"
                    ),
                    station_output_path=(
                        "processed/stations/"
                        "run-123"
                    ),
                    observation_output_path=(
                        "processed/"
                        "observations/"
                        "run-123"
                    ),
                )
            )
        ),
        execution_store=(
            LocalProcessingExecutionStore(
                events_root=tmp_path,
            )
        ),
        retry_store=(
            LocalProcessingRetryStore(
                events_root=tmp_path,
            )
        ),
        clock=lambda: (
            PROCESSING_TIME
        ),
        uuid_factory=lambda: (
            PROCESSING_EVENT_ID
        ),
    )

    worker.run_once()

    pending = (
        queue.list_pending()
    )

    assert len(pending) == 1

    completed = pending[0]

    assert (
        completed.event_id
        == PROCESSING_EVENT_ID
    )

    assert (
        completed.event_type
        is EventType.PROCESSING_COMPLETED
    )

    assert (
        completed.correlation_id
        == CORRELATION_ID
    )

    assert (
        completed.occurred_at
        == PROCESSING_TIME
    )

    assert dict(
        completed.data
    ) == {
        "provider": "bipad",
        "endpoint": "river-stations",
        "run_id": "run-123",
        "manifest_path": (
            "manifests/"
            "run-123/"
            "manifest.json"
        ),
        "quality_report_path": (
            "quality/run-123/"
            "report.json"
        ),
        "station_output_path": (
            "processed/stations/"
            "run-123"
        ),
        "observation_output_path": (
            "processed/observations/"
            "run-123"
        ),
        "ingestion_event_id": str(
            INGESTION_EVENT_ID
        ),
        "request_event_id": str(
            REQUEST_EVENT_ID
        ),
    }


def test_processing_worker_marks_ingestion_event_processed(
    tmp_path: Path,
) -> None:
    queue = LocalEventBus(
        events_root=tmp_path,
    )

    queue.publish(
        make_ingestion_completed_event()
    )

    worker = ProcessingWorker(
        queue=queue,
        runner=(
            RecordingProcessingRunner(
                ProcessingRunReference(
                    run_id="run-123",
                    quality_report_path=(
                        "quality/report.json"
                    ),
                    observation_output_path=(
                        "processed/"
                        "observations"
                    ),
                )
            )
        ),
        execution_store=(
            LocalProcessingExecutionStore(
                events_root=tmp_path,
            )
        ),
        retry_store=(
            LocalProcessingRetryStore(
                events_root=tmp_path,
            )
        ),
    )

    assert worker.run_once() == 1

    processed_path = (
        tmp_path
        / "processed"
        / f"{INGESTION_EVENT_ID}.json"
    )

    assert processed_path.is_file()


def test_processing_worker_ignores_other_events(
    tmp_path: Path,
) -> None:
    queue = LocalEventBus(
        events_root=tmp_path,
    )

    event = EventEnvelope(
        event_id=PROCESSING_EVENT_ID,
        event_type=(
            EventType.PROCESSING_COMPLETED
        ),
        occurred_at=PROCESSING_TIME,
        correlation_id=CORRELATION_ID,
        data={
            "run_id": "run-123",
        },
    )

    queue.publish(event)

    runner = (
        RecordingProcessingRunner(
            ProcessingRunReference(
                run_id="unused",
                quality_report_path="unused",
            )
        )
    )

    worker = ProcessingWorker(
        queue=queue,
        runner=runner,
        execution_store=(
            LocalProcessingExecutionStore(
                events_root=tmp_path,
            )
        ),
        retry_store=(
            LocalProcessingRetryStore(
                events_root=tmp_path,
            )
        ),
    )

    assert worker.run_once() == 0

    assert runner.calls == []

    assert (
        queue.list_pending()
        == [event]
    )


class FailingProcessingRunner:
    def run(
        self,
        *,
        provider: str,
        endpoint: str,
        run_id: str,
        manifest_path: str,
        idempotency_key: UUID,
    ) -> ProcessingRunReference:
        raise RuntimeError(
            "processing failed"
        )


def test_processing_worker_records_retry_on_failure(
    tmp_path: Path,
) -> None:
    queue = LocalEventBus(
        events_root=tmp_path,
    )

    queue.publish(
        make_ingestion_completed_event()
    )

    retry_store = (
        LocalProcessingRetryStore(
            events_root=tmp_path,
        )
    )

    worker = ProcessingWorker(
        queue=queue,
        runner=FailingProcessingRunner(),
        execution_store=(
            LocalProcessingExecutionStore(
                events_root=tmp_path,
            )
        ),
        retry_store=retry_store,
        clock=lambda: PROCESSING_TIME,
    )

    assert worker.run_once() == 0

    assert len(
        queue.list_pending()
    ) == 1

    state = retry_store.get(
        INGESTION_EVENT_ID
    )

    assert state is not None

    assert state.attempts == 1

    assert (
        state.last_error_type
        == "RuntimeError"
    )

    assert (
        state.last_error_message
        == "processing failed"
    )


def test_processing_worker_dead_letters_after_max_attempts(
    tmp_path: Path,
) -> None:
    queue = LocalEventBus(
        events_root=tmp_path,
    )

    queue.publish(
        make_ingestion_completed_event()
    )

    retry_store = (
        LocalProcessingRetryStore(
            events_root=tmp_path,
        )
    )

    worker = ProcessingWorker(
        queue=queue,
        runner=FailingProcessingRunner(),
        execution_store=(
            LocalProcessingExecutionStore(
                events_root=tmp_path,
            )
        ),
        retry_store=retry_store,
        retry_policy=RetryPolicy(
            max_attempts=3
        ),
        clock=lambda: PROCESSING_TIME,
    )

    assert worker.run_once() == 0
    assert worker.run_once() == 0
    assert worker.run_once() == 0

    assert queue.list_pending() == []

    assert (
        tmp_path
        / "failed"
        / f"{INGESTION_EVENT_ID}.json"
    ).is_file()

    state = retry_store.get(
        INGESTION_EVENT_ID
    )

    assert state is not None
    assert state.attempts == 3


def test_processing_worker_replays_saved_completion_without_rerun(
    tmp_path: Path,
) -> None:
    queue = LocalEventBus(
        events_root=tmp_path,
    )

    event = (
        make_ingestion_completed_event()
    )

    queue.publish(event)

    execution_store = (
        LocalProcessingExecutionStore(
            events_root=tmp_path,
        )
    )

    execution_store.save(
        ProcessingExecutionRecord(
            ingestion_event_id=(
                INGESTION_EVENT_ID
            ),
            completion_event_id=(
                PROCESSING_EVENT_ID
            ),
            correlation_id=(
                CORRELATION_ID
            ),
            request_event_id=(
                REQUEST_EVENT_ID
            ),
            provider="bipad",
            endpoint="river-stations",
            run_id="run-123",
            manifest_path=(
                "manifests/"
                "run-123/"
                "manifest.json"
            ),
            quality_report_path=(
                "quality/"
                "run-123/"
                "report.json"
            ),
            station_output_path=(
                "processed/stations/"
                "run-123"
            ),
            observation_output_path=(
                "processed/observations/"
                "run-123"
            ),
            occurred_at=PROCESSING_TIME,
        )
    )

    runner = (
        RecordingProcessingRunner(
            ProcessingRunReference(
                run_id="should-not-run",
                quality_report_path=(
                    "should-not-run"
                ),
            )
        )
    )

    worker = ProcessingWorker(
        queue=queue,
        runner=runner,
        execution_store=execution_store,
        retry_store=(
            LocalProcessingRetryStore(
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
        == PROCESSING_EVENT_ID
    )

    assert (
        pending[0].event_type
        is EventType.PROCESSING_COMPLETED
    )


class FailProcessingCompletionPublishOnceQueue:
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
            is EventType.PROCESSING_COMPLETED
            and not self._failed
        ):
            self._failed = True

            raise RuntimeError(
                "processing completion "
                "publish failed"
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


def test_processing_worker_recovers_after_completion_publish_failure(
    tmp_path: Path,
) -> None:
    local_queue = LocalEventBus(
        events_root=tmp_path,
    )

    local_queue.publish(
        make_ingestion_completed_event()
    )

    queue = (
        FailProcessingCompletionPublishOnceQueue(
            local_queue
        )
    )

    execution_store = (
        LocalProcessingExecutionStore(
            events_root=tmp_path,
        )
    )

    retry_store = (
        LocalProcessingRetryStore(
            events_root=tmp_path,
        )
    )

    runner = (
        RecordingProcessingRunner(
            ProcessingRunReference(
                run_id="run-123",
                quality_report_path=(
                    "quality/report.json"
                ),
                station_output_path=(
                    "processed/stations"
                ),
                observation_output_path=(
                    "processed/observations"
                ),
            )
        )
    )

    worker = ProcessingWorker(
        queue=queue,
        runner=runner,
        execution_store=execution_store,
        retry_store=retry_store,
        clock=lambda: PROCESSING_TIME,
        uuid_factory=lambda: (
            PROCESSING_EVENT_ID
        ),
    )

    assert worker.run_once() == 0

    assert len(runner.calls) == 1

    assert (
        execution_store.get(
            INGESTION_EVENT_ID
        )
        is not None
    )

    retry_state = retry_store.get(
        INGESTION_EVENT_ID
    )

    assert retry_state is not None
    assert retry_state.attempts == 1

    # Second delivery uses the saved
    # execution receipt. Spark/runner is
    # NOT called again.
    assert worker.run_once() == 1

    assert len(runner.calls) == 1

    pending = (
        local_queue.list_pending()
    )

    assert len(pending) == 1

    assert (
        pending[0].event_id
        == PROCESSING_EVENT_ID
    )

    assert (
        pending[0].event_type
        is EventType.PROCESSING_COMPLETED
    )

    assert (
        retry_store.get(
            INGESTION_EVENT_ID
        )
        is None
    )

def test_processing_worker_logs_lineage(
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
        make_ingestion_completed_event()
    )

    worker = ProcessingWorker(
        queue=queue,
        runner=(
            RecordingProcessingRunner(
                ProcessingRunReference(
                    run_id="run-123",
                    quality_report_path=(
                        "quality/report.json"
                    ),
                    station_output_path=(
                        "processed/stations"
                    ),
                    observation_output_path=(
                        "processed/observations"
                    ),
                )
            )
        ),
        execution_store=(
            LocalProcessingExecutionStore(
                events_root=tmp_path,
            )
        ),
        retry_store=(
            LocalProcessingRetryStore(
                events_root=tmp_path,
            )
        ),
        clock=lambda: PROCESSING_TIME,
        uuid_factory=lambda: (
            PROCESSING_EVENT_ID
        ),
        logger=logging.getLogger(
            "riverwatch.test.processing"
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
        "processing_started",
        "processing_completed",
    ]

    completed = documents[-1]

    assert (
        completed["event_id"]
        == str(INGESTION_EVENT_ID)
    )

    assert (
        completed["correlation_id"]
        == str(CORRELATION_ID)
    )

    assert (
        completed["request_event_id"]
        == str(REQUEST_EVENT_ID)
    )

    assert (
        completed["run_id"]
        == "run-123"
    )

    assert (
        completed[
            "quality_report_path"
        ]
        == "quality/report.json"
    )