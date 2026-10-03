from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from shutil import move
from uuid import UUID

from riverwatch.events.model import (
    EventEnvelope,
    EventType,
)

DEFAULT_EVENTS_ROOT = Path(
    "data/events"
)


class LocalEventBus:
    def __init__(
        self,
        *,
        events_root: Path | None = None,
    ) -> None:
        self.events_root = (
            DEFAULT_EVENTS_ROOT
            if events_root is None
            else events_root
        )

        self.pending_root = (
            self.events_root
            / "pending"
        )

        self.processed_root = (
            self.events_root
            / "processed"
        )

        self.failed_root = (
            self.events_root
            / "failed"
        )

        self.pending_root.mkdir(
            parents=True,
            exist_ok=True,
        )

        self.processed_root.mkdir(
            parents=True,
            exist_ok=True,
        )

        self.failed_root.mkdir(
            parents=True,
            exist_ok=True,
        )

    def publish(
        self,
        event: EventEnvelope,
    ) -> None:
        path = (
            self.pending_root
            / f"{event.event_id}.json"
        )

        payload = {
            "event_id": str(
                event.event_id
            ),
            "event_type": (
                event.event_type.value
            ),
            "occurred_at": (
                event.occurred_at
                .isoformat()
                .replace(
                    "+00:00",
                    "Z",
                )
            ),
            "correlation_id": str(
                event.correlation_id
            ),
            "schema_version": (
                event.schema_version
            ),
            "data": dict(
                event.data
            ),
        }

        try:
            with path.open(
                "x",
                encoding="utf-8",
            ) as file:
                json.dump(
                    payload,
                    file,
                    indent=2,
                )
        except FileExistsError:
            return

    def list_pending(
        self,
    ) -> list[EventEnvelope]:
        events: list[
            EventEnvelope
        ] = []

        for path in sorted(
            self.pending_root.glob(
                "*.json"
            )
        ):
            with path.open(
                "r",
                encoding="utf-8",
            ) as file:
                payload = json.load(
                    file
                )

            events.append(
                self._deserialize(
                    payload
                )
            )

        return events

    def mark_processed(
        self,
        event_id: UUID,
    ) -> None:
        source = (
            self.pending_root
            / f"{event_id}.json"
        )

        destination = (
            self.processed_root
            / f"{event_id}.json"
        )

        move(
            source,
            destination,
        )

    @staticmethod
    def _deserialize(
        payload: dict[str, object],
    ) -> EventEnvelope:
        occurred_at_value = payload[
            "occurred_at"
        ]

        if not isinstance(
            occurred_at_value,
            str,
        ):
            raise ValueError(
                "occurred_at must be a string"
            )

       # data_value = payload["data"]

        data_value = payload["data"]

        if not isinstance(
                data_value,
                dict,
        ):
            raise ValueError(
                "data must be an object"
            )

        schema_version_value = payload[
            "schema_version"
        ]

        if not isinstance(
                schema_version_value,
                int,
        ):
            raise ValueError(
                "schema_version must be an integer"
            )

        return EventEnvelope(
            event_id=UUID(
                str(
                    payload["event_id"]
                )
            ),
            event_type=EventType(
                str(
                    payload["event_type"]
                )
            ),
            occurred_at=(
                datetime.fromisoformat(
                    occurred_at_value.replace(
                        "Z",
                        "+00:00",
                    )
                )
            ),
            correlation_id=UUID(
                str(
                    payload[
                        "correlation_id"
                    ]
                )
            ),
            schema_version=(
                schema_version_value
            ),
            data=data_value,
        )

    def mark_failed(
            self,
            event_id: UUID,
    ) -> None:
        source = (
                self.events_root
                / "pending"
                / f"{event_id}.json"
        )

        destination = (
                self.events_root
                / "failed"
                / f"{event_id}.json"
        )

        destination.parent.mkdir(
            parents=True,
            exist_ok=True,
        )

        if destination.is_file():
            if not source.exists():
                return

            raise FileExistsError(
                "Failed event already exists: "
                f"{destination}"
            )

        source.replace(
            destination
        )