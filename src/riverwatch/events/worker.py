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
from riverwatch.events.idempotency import (
    IdempotencyConflictError,
    IngestionExecutionRecord,
    IngestionExecutionStore,
)
from riverwatch.events.model import (
    EventEnvelope,
    EventType,
)
from riverwatch.events.retry import (
    IngestionRetryStore,
    RetryPolicy,
)
from riverwatch.observability.logging import (
    log_event,
)

Clock = Callable[[], datetime]
UUIDFactory = Callable[[], UUID]


def utc_now() -> datetime:
    return datetime.now(
        UTC
    )


@dataclass(
    frozen=True,
    slots=True,
)
class IngestionRunReference:
    run_id: str
    manifest_path: str

    def __post_init__(
        self,
    ) -> None:
        if not self.run_id.strip():
            raise ValueError(
                "run_id must not be blank"
            )

        if not self.manifest_path.strip():
            raise ValueError(
                "manifest_path must not "
                "be blank"
            )


class IngestionRunner(
    Protocol,
):
    def run(
        self,
        *,
        provider: str,
        endpoint: str,
        idempotency_key: UUID,
    ) -> IngestionRunReference:
        """Run one idempotent ingestion request."""
        ...


class IngestionWorker:
    def __init__(
            self,
            *,
            queue: EventQueue,
            runner: IngestionRunner,
            execution_store: (
                    IngestionExecutionStore
            ),
            retry_store: IngestionRetryStore,
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
        self._retry_store = (
            retry_store
        )
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
                "riverwatch.events.ingestion_worker"
            )
        )

    def run_once(
            self,
    ) -> int:
        pending_events = (
            self._queue.list_pending()
        )

        processed_count = 0

        for event in pending_events:
            if (
                    event.event_type
                    != EventType
                    .INGESTION_REQUESTED
            ):
                continue

            try:
                self._process_request(
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

    def _process_request(
        self,
        event: EventEnvelope,
    ) -> None:
        provider = self._required_text(
            event,
            "provider",
        )

        endpoint = self._required_text(
            event,
            "endpoint",
        )
        log_event(
            self._logger,
            logging.INFO,
            "ingestion_request_started",
            event_id=str(
                event.event_id
            ),
            correlation_id=str(
                event.correlation_id
            ),
            provider=provider,
            endpoint=endpoint,
        )

        existing = (
            self._execution_store.get(
                event.event_id
            )
        )

        if existing is not None:
            self._validate_existing_record(
                record=existing,
                request=event,
                provider=provider,
                endpoint=endpoint,
            )
            log_event(
                self._logger,
                logging.INFO,
                "ingestion_request_replayed",
                event_id=str(
                    event.event_id
                ),
                correlation_id=str(
                    event.correlation_id
                ),
                completion_event_id=str(
                    existing
                    .completion_event_id
                ),
                provider=provider,
                endpoint=endpoint,
                run_id=existing.run_id,
                manifest_path=(
                    existing.manifest_path
                ),
            )

            self._complete_from_record(
                request=event,
                record=existing,
            )

            return

        result = self._runner.run(
            provider=provider,
            endpoint=endpoint,
            idempotency_key=(
                event.event_id
            ),
        )

        record = (
            IngestionExecutionRecord(
                request_event_id=(
                    event.event_id
                ),
                completion_event_id=(
                    self._uuid_factory()
                ),
                correlation_id=(
                    event.correlation_id
                ),
                provider=provider,
                endpoint=endpoint,
                run_id=(
                    result.run_id
                ),
                manifest_path=(
                    result.manifest_path
                ),
                occurred_at=(
                    self._clock()
                ),
            )
        )

        self._execution_store.save(
            record
        )

        self._complete_from_record(
            request=event,
            record=record,
        )

    def _complete_from_record(
        self,
        *,
        request: EventEnvelope,
        record: IngestionExecutionRecord,
    ) -> None:
        self._queue.publish(
            record.to_event()
        )

        self._queue.mark_processed(
            request.event_id
        )
        log_event(
            self._logger,
            logging.INFO,
            "ingestion_request_completed",
            event_id=str(
                request.event_id
            ),
            correlation_id=str(
                request.correlation_id
            ),
            completion_event_id=str(
                record.completion_event_id
            ),
            provider=record.provider,
            endpoint=record.endpoint,
            run_id=record.run_id,
            manifest_path=(
                record.manifest_path
            ),
        )

    @staticmethod
    def _validate_existing_record(
        *,
        record: IngestionExecutionRecord,
        request: EventEnvelope,
        provider: str,
        endpoint: str,
    ) -> None:
        if (
            record.request_event_id
            != request.event_id
        ):
            raise (
                IdempotencyConflictError(
                    "Execution record "
                    "request ID does not "
                    "match event"
                )
            )

        if (
            record.correlation_id
            != request.correlation_id
        ):
            raise (
                IdempotencyConflictError(
                    "Execution record "
                    "correlation ID does "
                    "not match request"
                )
            )

        if (
            record.provider
            != provider
        ):
            raise (
                IdempotencyConflictError(
                    "Execution record "
                    "provider does not "
                    "match request"
                )
            )

        if (
            record.endpoint
            != endpoint
        ):
            raise (
                IdempotencyConflictError(
                    "Execution record "
                    "endpoint does not "
                    "match request"
                )
            )

    @staticmethod
    def _required_text(
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
                f"{key} must be a string"
            )

        normalized = (
            value.strip()
        )

        if not normalized:
            raise ValueError(
                f"{key} must not be blank"
            )

        return normalized

    def _handle_failure(
            self,
            *,
            event: EventEnvelope,
            error: Exception,
    ) -> None:
        error_message = str(
            error
        )

        if not error_message:
            error_message = (
                error.__class__.__name__
            )

        state = (
            self._retry_store
            .record_failure(
                request_event_id=(
                    event.event_id
                ),
                failed_at=(
                    self._clock()
                ),
                error_type=(
                    error
                    .__class__
                    .__name__
                ),
                error_message=(
                    error_message
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
            "ingestion_request_failed",
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
            attempt=state.attempts,
            max_attempts=(
                self._retry_policy.max_attempts
            ),
            dead_lettered=dead_lettered,
            error_type=(
                error
                .__class__
                .__name__
            ),
            error_message=(
                error_message
            ),
        )