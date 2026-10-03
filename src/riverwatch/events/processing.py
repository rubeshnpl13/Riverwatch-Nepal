from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Protocol
from uuid import UUID, uuid4

from riverwatch.events.bus import (
    EventQueue,
)
from riverwatch.events.model import (
    EventEnvelope,
    EventType,
)
from riverwatch.events.processing_idempotency import (
    ProcessingExecutionConflictError,
    ProcessingExecutionRecord,
    ProcessingExecutionStore,
)
from riverwatch.events.processing_retry import (
    ProcessingRetryStore,
)
from riverwatch.events.retry import (
    RetryPolicy,
)
from riverwatch.observability.logging import (
    log_event,
)

Clock = Callable[
    [],
    datetime,
]

UUIDFactory = Callable[
    [],
    UUID,
]


def utc_now() -> datetime:
    return datetime.now(UTC)


@dataclass(
    frozen=True,
    slots=True,
)
class ProcessingRunReference:
    run_id: str
    quality_report_path: str
    station_output_path: str | None = None
    observation_output_path: str | None = None

    def __post_init__(self) -> None:
        run_id = self.run_id.strip()

        quality_report_path = (
            self.quality_report_path.strip()
        )

        if not run_id:
            raise ValueError(
                "run_id must not be blank"
            )

        if not quality_report_path:
            raise ValueError(
                "quality_report_path must "
                "not be blank"
            )

        station_output_path = (
            _clean_optional_text(
                self.station_output_path
            )
        )

        observation_output_path = (
            _clean_optional_text(
                self.observation_output_path
            )
        )

        object.__setattr__(
            self,
            "run_id",
            run_id,
        )

        object.__setattr__(
            self,
            "quality_report_path",
            quality_report_path,
        )

        object.__setattr__(
            self,
            "station_output_path",
            station_output_path,
        )

        object.__setattr__(
            self,
            "observation_output_path",
            observation_output_path,
        )


class ProcessingRunner(Protocol):
    def run(
        self,
        *,
        provider: str,
        endpoint: str,
        run_id: str,
        manifest_path: str,
        idempotency_key: UUID,
    ) -> ProcessingRunReference:
        """Process one completed ingestion run."""


class ProcessingWorker:
    def __init__(
        self,
        *,
        queue: EventQueue,
        runner: ProcessingRunner,
        execution_store: ProcessingExecutionStore,
        retry_store: ProcessingRetryStore,
        retry_policy: RetryPolicy | None = None,
        clock: Clock = utc_now,
        uuid_factory: UUIDFactory = uuid4,
        logger: logging.Logger | None = None,
    ) -> None:
        self._queue = queue
        self._runner = runner
        self._execution_store = (
            execution_store
        )
        self._retry_store = retry_store

        self._retry_policy = (
            retry_policy
            if retry_policy is not None
            else RetryPolicy()
        )

        self._clock = clock
        self._uuid_factory = uuid_factory

        self._logger = (
            logger
            if logger is not None
            else logging.getLogger(
                "riverwatch.events.processing_worker"
            )
        )

    def run_once(self) -> int:
        processed_count = 0

        for event in (
            self._queue.list_pending()
        ):
            if (
                event.event_type
                is not EventType.INGESTION_COMPLETED
            ):
                continue

            try:
                self._process_event(
                    event
                )

            except Exception as exc:
                self._handle_failure(
                    event=event,
                    error=exc,
                )

                continue

            self._retry_store.clear(
                event.event_id
            )

            processed_count += 1

        return processed_count

    def _process_event(
        self,
        event: EventEnvelope,
    ) -> None:
        provider = _required_text(
            event=event,
            key="provider",
        )

        endpoint = _required_text(
            event=event,
            key="endpoint",
        )

        run_id = _required_text(
            event=event,
            key="run_id",
        )

        manifest_path = _required_text(
            event=event,
            key="manifest_path",
        )

        request_event_id = (
            _required_uuid(
                event=event,
                key="request_event_id",
            )
        )

        log_event(
            self._logger,
            logging.INFO,
            "processing_started",
            event_id=str(
                event.event_id
            ),
            correlation_id=str(
                event.correlation_id
            ),
            request_event_id=str(
                request_event_id
            ),
            provider=provider,
            endpoint=endpoint,
            run_id=run_id,
            manifest_path=manifest_path,
        )

        existing = (
            self._execution_store.get(
                event.event_id
            )
        )

        if existing is not None:
            self._validate_existing_record(
                event=event,
                record=existing,
                provider=provider,
                endpoint=endpoint,
                run_id=run_id,
                manifest_path=manifest_path,
                request_event_id=(
                    request_event_id
                ),
            )

            log_event(
                self._logger,
                logging.INFO,
                "processing_replayed",
                event_id=str(
                    event.event_id
                ),
                correlation_id=str(
                    event.correlation_id
                ),
                completion_event_id=str(
                    existing.completion_event_id
                ),
                request_event_id=str(
                    request_event_id
                ),
                provider=provider,
                endpoint=endpoint,
                run_id=run_id,
                manifest_path=manifest_path,
                quality_report_path=(
                    existing.quality_report_path
                ),
            )

            self._complete_from_record(
                event=event,
                record=existing,
            )

            return

        result = self._runner.run(
            provider=provider,
            endpoint=endpoint,
            run_id=run_id,
            manifest_path=manifest_path,
            idempotency_key=event.event_id,
        )

        if result.run_id != run_id:
            raise ValueError(
                "Processing runner returned "
                "an unexpected run_id"
            )

        record = ProcessingExecutionRecord(
            ingestion_event_id=(
                event.event_id
            ),
            completion_event_id=(
                self._uuid_factory()
            ),
            correlation_id=(
                event.correlation_id
            ),
            request_event_id=(
                request_event_id
            ),
            provider=provider,
            endpoint=endpoint,
            run_id=run_id,
            manifest_path=manifest_path,
            quality_report_path=(
                result.quality_report_path
            ),
            station_output_path=(
                result.station_output_path
            ),
            observation_output_path=(
                result.observation_output_path
            ),
            occurred_at=self._clock(),
        )

        self._execution_store.save(
            record
        )

        self._complete_from_record(
            event=event,
            record=record,
        )

    def _complete_from_record(
        self,
        *,
        event: EventEnvelope,
        record: ProcessingExecutionRecord,
    ) -> None:
        self._queue.publish(
            record.to_event()
        )

        self._queue.mark_processed(
            event.event_id
        )

        log_event(
            self._logger,
            logging.INFO,
            "processing_completed",
            event_id=str(
                event.event_id
            ),
            correlation_id=str(
                event.correlation_id
            ),
            completion_event_id=str(
                record.completion_event_id
            ),
            request_event_id=str(
                record.request_event_id
            ),
            provider=record.provider,
            endpoint=record.endpoint,
            run_id=record.run_id,
            manifest_path=(
                record.manifest_path
            ),
            station_output_path=(
                record.station_output_path
            ),
            observation_output_path=(
                record.observation_output_path
            ),
            quality_report_path=(
                record.quality_report_path
            ),
        )

    def _validate_existing_record(
        self,
        *,
        event: EventEnvelope,
        record: ProcessingExecutionRecord,
        provider: str,
        endpoint: str,
        run_id: str,
        manifest_path: str,
        request_event_id: UUID,
    ) -> None:
        if (
            record.ingestion_event_id
            != event.event_id
            or record.correlation_id
            != event.correlation_id
            or record.request_event_id
            != request_event_id
            or record.provider
            != provider
            or record.endpoint
            != endpoint
            or record.run_id
            != run_id
            or record.manifest_path
            != manifest_path
        ):
            raise (
                ProcessingExecutionConflictError(
                    "Stored processing execution "
                    "does not match the "
                    "ingestion event"
                )
            )

    def _handle_failure(
        self,
        *,
        event: EventEnvelope,
        error: Exception,
    ) -> None:
        state = (
            self._retry_store
            .record_failure(
                ingestion_event_id=(
                    event.event_id
                ),
                failed_at=self._clock(),
                error_type=(
                    type(error).__name__
                ),
                error_message=str(
                    error
                ),
            )
        )

        dead_lettered = (
            state.attempts
            >= self._retry_policy.max_attempts
        )

        if dead_lettered:
            self._queue.mark_failed(
                event.event_id
            )

        log_event(
            self._logger,
            logging.WARNING,
            "processing_failed",
            event_id=str(
                event.event_id
            ),
            correlation_id=str(
                event.correlation_id
            ),
            provider=event.data.get(
                "provider"
            ),
            endpoint=event.data.get(
                "endpoint"
            ),
            run_id=event.data.get(
                "run_id"
            ),
            attempt=state.attempts,
            max_attempts=(
                self._retry_policy.max_attempts
            ),
            dead_lettered=dead_lettered,
            error_type=(
                type(error).__name__
            ),
            error_message=str(
                error
            ),
        )


def _required_text(
    *,
    event: EventEnvelope,
    key: str,
) -> str:
    value = event.data.get(
        key
    )

    if not isinstance(
        value,
        str,
    ):
        raise ValueError(
            "Event field must be a "
            f"string: {key}"
        )

    cleaned = value.strip()

    if not cleaned:
        raise ValueError(
            "Event field must not be "
            f"blank: {key}"
        )

    return cleaned


def _required_uuid(
    *,
    event: EventEnvelope,
    key: str,
) -> UUID:
    value = _required_text(
        event=event,
        key=key,
    )

    try:
        return UUID(
            value
        )

    except ValueError as exc:
        raise ValueError(
            f"Event field must be a UUID: {key}"
        ) from exc


def _clean_optional_text(
    value: str | None,
) -> str | None:
    if value is None:
        return None

    cleaned = value.strip()

    if not cleaned:
        raise ValueError(
            "Optional path must not "
            "be blank"
        )

    return cleaned