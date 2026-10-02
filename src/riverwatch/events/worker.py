from __future__ import annotations

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
                "manifest_path must not be blank"
            )


class IngestionRunner(
    Protocol,
):
    def run(
        self,
        *,
        provider: str,
        endpoint: str,
    ) -> IngestionRunReference:
        """Run one ingestion request."""
        ...


class IngestionWorker:
    def __init__(
        self,
        *,
        queue: EventQueue,
        runner: IngestionRunner,
        clock: Clock = utc_now,
        uuid_factory: UUIDFactory = uuid4,
    ) -> None:
        self._queue = queue
        self._runner = runner
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

        result = self._runner.run(
            provider=provider,
            endpoint=endpoint,
        )

        completed_event = (
            EventEnvelope(
                event_id=(
                    self._uuid_factory()
                ),
                event_type=(
                    EventType
                    .INGESTION_COMPLETED
                ),
                occurred_at=(
                    self._clock()
                ),
                correlation_id=(
                    event.correlation_id
                ),
                data={
                    "provider":
                        provider,

                    "endpoint":
                        endpoint,

                    "run_id":
                        result.run_id,

                    "manifest_path":
                        result.manifest_path,

                    "request_event_id":
                        str(
                            event.event_id
                        ),
                },
            )
        )

        self._queue.publish(
            completed_event
        )

        self._queue.mark_processed(
            event.event_id
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

        normalized = value.strip()

        if not normalized:
            raise ValueError(
                f"{key} must not be blank"
            )

        return normalized