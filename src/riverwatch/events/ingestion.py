from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime
from uuid import UUID, uuid4

from riverwatch.events.bus import EventPublisher
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


class IngestionRequestPublisher:
    def __init__(
        self,
        *,
        publisher: EventPublisher,
        clock: Clock = utc_now,
        uuid_factory: UUIDFactory = uuid4,
    ) -> None:
        self._publisher = publisher
        self._clock = clock
        self._uuid_factory = uuid_factory

    def request(
        self,
        *,
        provider: str,
        endpoint: str,
        correlation_id: UUID | None = None,
    ) -> EventEnvelope:
        normalized_provider = (
            provider.strip()
        )

        normalized_endpoint = (
            endpoint.strip()
        )

        if not normalized_provider:
            raise ValueError(
                "provider must not be blank"
            )

        if not normalized_endpoint:
            raise ValueError(
                "endpoint must not be blank"
            )

        event = EventEnvelope(
            event_id=(
                self._uuid_factory()
            ),
            event_type=(
                EventType
                .INGESTION_REQUESTED
            ),
            occurred_at=(
                self._clock()
            ),
            correlation_id=(
                correlation_id
                if correlation_id
                is not None
                else self._uuid_factory()
            ),
            data={
                "provider":
                    normalized_provider,

                "endpoint":
                    normalized_endpoint,
            },
        )

        self._publisher.publish(
            event
        )

        return event