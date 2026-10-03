from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
from types import MappingProxyType
from uuid import UUID

type EventScalar = (
    str
    | int
    | float
    | bool
    | None
)

type EventData = Mapping[
    str,
    EventScalar,
]


class EventType(StrEnum):
    INGESTION_REQUESTED = (
        "ingestion.requested"
    )
    INGESTION_COMPLETED = (
        "ingestion.completed"
    )
    PROCESSING_COMPLETED = (
        "processing.completed"
    )


@dataclass(
    frozen=True,
    slots=True,
)
class EventEnvelope:
    event_id: UUID
    event_type: EventType
    occurred_at: datetime
    correlation_id: UUID
    data: EventData
    schema_version: int = 1

    def __post_init__(
        self,
    ) -> None:
        if (
            self.occurred_at.tzinfo
            is None
            or self.occurred_at.utcoffset()
            is None
        ):
            raise ValueError(
                "occurred_at must be "
                "timezone-aware"
            )

        if self.schema_version < 1:
            raise ValueError(
                "schema_version must be "
                "at least 1"
            )

        object.__setattr__(
            self,
            "occurred_at",
            self.occurred_at.astimezone(
                UTC
            ),
        )

        object.__setattr__(
            self,
            "data",
            MappingProxyType(
                dict(
                    self.data
                )
            ),
        )