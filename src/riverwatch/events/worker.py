from __future__ import annotations

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
        clock: Clock = utc_now,
        uuid_factory: UUIDFactory = uuid4,
    ) -> None:
        self._queue = queue
        self._runner = runner
        self._execution_store = (
            execution_store
        )
        self._clock = clock
        self._uuid_factory = uuid_factory

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

            self._process_request(
                event
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