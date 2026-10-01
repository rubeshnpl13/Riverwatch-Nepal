from __future__ import annotations

from typing import Protocol

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