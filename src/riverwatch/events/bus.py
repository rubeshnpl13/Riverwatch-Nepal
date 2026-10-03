from __future__ import annotations

from typing import Protocol
from uuid import UUID

from riverwatch.events.model import (
    EventEnvelope,
)


class EventPublisher(
    Protocol,
):
    def publish(
        self,
        event: EventEnvelope,
    ) -> None:
        """Publish an event."""
        ...


class EventQueue(
    EventPublisher,
    Protocol,
):
    def list_pending(
        self,
    ) -> list[EventEnvelope]:
        """Return pending events."""
        ...

    def mark_processed(
        self,
        event_id: UUID,
    ) -> None:
        """Move an event to processed."""
        ...

    def mark_failed(
        self,
        event_id: UUID,
    ) -> None:
        """Move an event to the dead-letter queue."""
        ...