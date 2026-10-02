from __future__ import annotations

import json
import os
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Protocol
from uuid import UUID

from riverwatch.events.model import (
    EventEnvelope,
    EventType,
)


class IdempotencyRecordError(RuntimeError):
    """An idempotency record could not be read."""


class IdempotencyConflictError(RuntimeError):
    """A request ID was associated with conflicting execution results."""


@dataclass(
    frozen=True,
    slots=True,
)
class IngestionExecutionRecord:
    request_event_id: UUID
    completion_event_id: UUID
    correlation_id: UUID

    provider: str
    endpoint: str

    run_id: str
    manifest_path: str

    occurred_at: datetime

    def __post_init__(self) -> None:
        if not self.provider.strip():
            raise ValueError(
                "provider must not be blank"
            )

        if not self.endpoint.strip():
            raise ValueError(
                "endpoint must not be blank"
            )

        if not self.run_id.strip():
            raise ValueError(
                "run_id must not be blank"
            )

        if not self.manifest_path.strip():
            raise ValueError(
                "manifest_path must not be blank"
            )

        if (
            self.occurred_at.tzinfo is None
            or self.occurred_at.utcoffset()
            is None
        ):
            raise ValueError(
                "occurred_at must be timezone-aware"
            )

        object.__setattr__(
            self,
            "provider",
            self.provider.strip(),
        )

        object.__setattr__(
            self,
            "endpoint",
            self.endpoint.strip(),
        )

        object.__setattr__(
            self,
            "run_id",
            self.run_id.strip(),
        )

        object.__setattr__(
            self,
            "manifest_path",
            self.manifest_path.strip(),
        )

        object.__setattr__(
            self,
            "occurred_at",
            self.occurred_at.astimezone(
                UTC
            ),
        )

    def to_event(
        self,
    ) -> EventEnvelope:
        return EventEnvelope(
            event_id=(
                self.completion_event_id
            ),
            event_type=(
                EventType
                .INGESTION_COMPLETED
            ),
            occurred_at=(
                self.occurred_at
            ),
            correlation_id=(
                self.correlation_id
            ),
            data={
                "provider":
                    self.provider,

                "endpoint":
                    self.endpoint,

                "run_id":
                    self.run_id,

                "manifest_path":
                    self.manifest_path,

                "request_event_id":
                    str(
                        self.request_event_id
                    ),
            },
        )


class IngestionExecutionStore(
    Protocol,
):
    def get(
        self,
        request_event_id: UUID,
    ) -> (
        IngestionExecutionRecord
        | None
    ):
        """Return an existing execution record."""
        ...

    def save(
        self,
        record: IngestionExecutionRecord,
    ) -> None:
        """Persist an execution record once."""
        ...


class LocalIngestionExecutionStore:
    def __init__(
        self,
        *,
        events_root: Path,
    ) -> None:
        self._root = (
            events_root
            / "idempotency"
            / "ingestion"
        )

        self._root.mkdir(
            parents=True,
            exist_ok=True,
        )

    def get(
        self,
        request_event_id: UUID,
    ) -> (
        IngestionExecutionRecord
        | None
    ):
        path = self._path_for(
            request_event_id
        )

        if not path.is_file():
            return None

        try:
            document = json.loads(
                path.read_text(
                    encoding="utf-8"
                )
            )

            return (
                self._deserialize(
                    document
                )
            )

        except (
            OSError,
            KeyError,
            TypeError,
            ValueError,
            json.JSONDecodeError,
        ) as exc:
            raise IdempotencyRecordError(
                "Unable to read ingestion "
                "idempotency record: "
                f"{path}"
            ) from exc

    def save(
        self,
        record: IngestionExecutionRecord,
    ) -> None:
        path = self._path_for(
            record.request_event_id
        )

        document = {
            "request_event_id":
                str(
                    record.request_event_id
                ),

            "completion_event_id":
                str(
                    record.completion_event_id
                ),

            "correlation_id":
                str(
                    record.correlation_id
                ),

            "provider":
                record.provider,

            "endpoint":
                record.endpoint,

            "run_id":
                record.run_id,

            "manifest_path":
                record.manifest_path,

            "occurred_at":
                self._serialize_datetime(
                    record.occurred_at
                ),
        }

        serialized = (
            json.dumps(
                document,
                sort_keys=True,
                separators=(
                    ",",
                    ":",
                ),
            )
            + "\n"
        )

        try:
            with path.open(
                "x",
                encoding="utf-8",
            ) as handle:
                handle.write(
                    serialized
                )

                handle.flush()

                os.fsync(
                    handle.fileno()
                )

        except FileExistsError:
            existing = self.get(
                record.request_event_id
            )

            if existing == record:
                return

            raise  IdempotencyConflictError(
                    "Conflicting ingestion "
                    "execution record for "
                    f"{record.request_event_id}"
                ) from None


    def _path_for(
        self,
        request_event_id: UUID,
    ) -> Path:
        return (
            self._root
            / (
                f"{request_event_id}"
                ".json"
            )
        )

    @staticmethod
    def _serialize_datetime(
        value: datetime,
    ) -> str:
        return (
            value
            .astimezone(UTC)
            .isoformat()
            .replace(
                "+00:00",
                "Z",
            )
        )

    @staticmethod
    def _deserialize(
        document: object,
    ) -> IngestionExecutionRecord:
        if not isinstance(
            document,
            dict,
        ):
            raise ValueError(
                "Idempotency record "
                "must be an object"
            )

        occurred_at_raw = (
            document[
                "occurred_at"
            ]
        )

        if not isinstance(
            occurred_at_raw,
            str,
        ):
            raise ValueError(
                "occurred_at must "
                "be a string"
            )

        occurred_at = (
            datetime.fromisoformat(
                occurred_at_raw.replace(
                    "Z",
                    "+00:00",
                )
            )
        )

        return (
            IngestionExecutionRecord(
                request_event_id=UUID(
                    str(
                        document[
                            "request_event_id"
                        ]
                    )
                ),
                completion_event_id=UUID(
                    str(
                        document[
                            "completion_event_id"
                        ]
                    )
                ),
                correlation_id=UUID(
                    str(
                        document[
                            "correlation_id"
                        ]
                    )
                ),
                provider=str(
                    document[
                        "provider"
                    ]
                ),
                endpoint=str(
                    document[
                        "endpoint"
                    ]
                ),
                run_id=str(
                    document[
                        "run_id"
                    ]
                ),
                manifest_path=str(
                    document[
                        "manifest_path"
                    ]
                ),
                occurred_at=occurred_at,
            )
        )