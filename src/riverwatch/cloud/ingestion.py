from __future__ import annotations

from collections.abc import Callable
from datetime import (
    UTC,
    datetime,
)
from uuid import (
    NAMESPACE_URL,
    UUID,
    uuid5,
)

from riverwatch.events.bus import (
    EventPublisher,
)
from riverwatch.events.idempotency import (
    IngestionExecutionRecord,
)
from riverwatch.events.model import (
    EventEnvelope,
    EventType,
)
from riverwatch.events.worker import (
    IngestionRunner,
)

Clock = Callable[[], datetime]


def utc_now() -> datetime:
    return datetime.now(
        UTC
    )


def ingestion_completion_event_id(
    request_event_id: UUID,
) -> UUID:
    return uuid5(
        NAMESPACE_URL,
        (
            "riverwatch:"
            "ingestion-completed:"
            f"{request_event_id}"
        ),
    )


class CloudIngestionHandler:
    """Handle one ingestion.requested event."""

    def __init__(
        self,
        *,
        runner: IngestionRunner,
        publisher: EventPublisher,
        clock: Clock = utc_now,
    ) -> None:
        self._runner = runner
        self._publisher = publisher
        self._clock = clock

    def handle(
        self,
        event: EventEnvelope,
    ) -> EventEnvelope:
        if (
            event.event_type
            is not EventType.INGESTION_REQUESTED
        ):
            raise ValueError(
                "Cloud ingestion handler "
                "requires an "
                "ingestion.requested event"
            )

        provider = self._required_text(
            event,
            "provider",
        )

        endpoint = self._required_text(
            event,
            "endpoint",
        )

        result = self._runner.run(
            provider=provider,
            endpoint=endpoint,
            idempotency_key=(
                event.event_id
            ),
        )

        occurred_at = (
            result.completed_at
            if result.completed_at
            is not None
            else self._clock()
        )

        record = IngestionExecutionRecord(
            request_event_id=(
                event.event_id
            ),
            completion_event_id=(
                ingestion_completion_event_id(
                    event.event_id
                )
            ),
            correlation_id=(
                event.correlation_id
            ),
            provider=provider,
            endpoint=endpoint,
            run_id=result.run_id,
            manifest_path=(
                result.manifest_path
            ),
            occurred_at=occurred_at,
        )

        completed = record.to_event()

        self._publisher.publish(
            completed
        )

        return completed

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

        normalized = value.strip()

        if not normalized:
            raise ValueError(
                f"{key} must not be blank"
            )

        return normalized