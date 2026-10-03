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


class ProcessingExecutionRecordError(
    RuntimeError
):
    """Stored processing execution record is invalid."""


class ProcessingExecutionConflictError(
    RuntimeError
):
    """A conflicting processing execution already exists."""


@dataclass(
    frozen=True,
    slots=True,
)
class ProcessingExecutionRecord:
    ingestion_event_id: UUID
    completion_event_id: UUID
    correlation_id: UUID
    request_event_id: UUID

    provider: str
    endpoint: str
    run_id: str
    manifest_path: str

    quality_report_path: str
    station_output_path: str | None
    observation_output_path: str | None

    occurred_at: datetime

    def __post_init__(self) -> None:
        for field_name in (
            "provider",
            "endpoint",
            "run_id",
            "manifest_path",
            "quality_report_path",
        ):
            value = getattr(
                self,
                field_name,
            )

            cleaned = value.strip()

            if not cleaned:
                raise ValueError(
                    f"{field_name} must not be blank"
                )

            object.__setattr__(
                self,
                field_name,
                cleaned,
            )

        object.__setattr__(
            self,
            "station_output_path",
            _clean_optional_text(
                self.station_output_path
            ),
        )

        object.__setattr__(
            self,
            "observation_output_path",
            _clean_optional_text(
                self.observation_output_path
            ),
        )

        if self.occurred_at.tzinfo is None:
            raise ValueError(
                "occurred_at must be timezone-aware"
            )

        object.__setattr__(
            self,
            "occurred_at",
            self.occurred_at.astimezone(
                UTC
            ),
        )

    def to_event(self) -> EventEnvelope:
        return EventEnvelope(
            event_id=self.completion_event_id,
            event_type=(
                EventType.PROCESSING_COMPLETED
            ),
            occurred_at=self.occurred_at,
            correlation_id=self.correlation_id,
            data={
                "provider": self.provider,
                "endpoint": self.endpoint,
                "run_id": self.run_id,
                "manifest_path": (
                    self.manifest_path
                ),
                "quality_report_path": (
                    self.quality_report_path
                ),
                "station_output_path": (
                    self.station_output_path
                ),
                "observation_output_path": (
                    self.observation_output_path
                ),
                "ingestion_event_id": str(
                    self.ingestion_event_id
                ),
                "request_event_id": str(
                    self.request_event_id
                ),
            },
        )


class ProcessingExecutionStore(
    Protocol
):
    def get(
        self,
        ingestion_event_id: UUID,
    ) -> ProcessingExecutionRecord | None:
        """Return a stored processing execution."""
        ...

    def save(
        self,
        record: ProcessingExecutionRecord,
    ) -> None:
        """Persist one processing execution."""
        ...


class LocalProcessingExecutionStore:
    def __init__(
        self,
        *,
        events_root: str | Path,
    ) -> None:
        self.events_root = Path(
            events_root
        )

    def get(
        self,
        ingestion_event_id: UUID,
    ) -> ProcessingExecutionRecord | None:
        path = self._path(
            ingestion_event_id
        )

        if not path.is_file():
            return None

        try:
            document = json.loads(
                path.read_text(
                    encoding="utf-8"
                )
            )

        except (
            OSError,
            json.JSONDecodeError,
        ) as exc:
            raise ProcessingExecutionRecordError(
                "Unable to read processing "
                f"execution record: {path}"
            ) from exc

        if not isinstance(
            document,
            dict,
        ):
            raise ProcessingExecutionRecordError(
                "Processing execution record "
                "must be a JSON object"
            )

        try:
            return ProcessingExecutionRecord(
                ingestion_event_id=UUID(
                    _required_string(
                        document,
                        "ingestion_event_id",
                    )
                ),
                completion_event_id=UUID(
                    _required_string(
                        document,
                        "completion_event_id",
                    )
                ),
                correlation_id=UUID(
                    _required_string(
                        document,
                        "correlation_id",
                    )
                ),
                request_event_id=UUID(
                    _required_string(
                        document,
                        "request_event_id",
                    )
                ),
                provider=_required_string(
                    document,
                    "provider",
                ),
                endpoint=_required_string(
                    document,
                    "endpoint",
                ),
                run_id=_required_string(
                    document,
                    "run_id",
                ),
                manifest_path=_required_string(
                    document,
                    "manifest_path",
                ),
                quality_report_path=(
                    _required_string(
                        document,
                        "quality_report_path",
                    )
                ),
                station_output_path=(
                    _optional_string(
                        document,
                        "station_output_path",
                    )
                ),
                observation_output_path=(
                    _optional_string(
                        document,
                        "observation_output_path",
                    )
                ),
                occurred_at=_parse_datetime(
                    _required_string(
                        document,
                        "occurred_at",
                    )
                ),
            )

        except (
            ValueError,
            TypeError,
        ) as exc:
            raise ProcessingExecutionRecordError(
                "Invalid processing "
                f"execution record: {path}"
            ) from exc

    def save(
        self,
        record: ProcessingExecutionRecord,
    ) -> None:
        path = self._path(
            record.ingestion_event_id
        )

        existing = self.get(
            record.ingestion_event_id
        )

        if existing is not None:
            if existing == record:
                return

            raise ProcessingExecutionConflictError(
                "Conflicting processing "
                "execution record for "
                f"{record.ingestion_event_id}"
            )

        path.parent.mkdir(
            parents=True,
            exist_ok=True,
        )

        document = {
            "ingestion_event_id": str(
                record.ingestion_event_id
            ),
            "completion_event_id": str(
                record.completion_event_id
            ),
            "correlation_id": str(
                record.correlation_id
            ),
            "request_event_id": str(
                record.request_event_id
            ),
            "provider": record.provider,
            "endpoint": record.endpoint,
            "run_id": record.run_id,
            "manifest_path": (
                record.manifest_path
            ),
            "quality_report_path": (
                record.quality_report_path
            ),
            "station_output_path": (
                record.station_output_path
            ),
            "observation_output_path": (
                record.observation_output_path
            ),
            "occurred_at": _format_datetime(
                record.occurred_at
            ),
        }

        data = (
            json.dumps(
                document,
                sort_keys=True,
                separators=(",", ":"),
            )
            + "\n"
        )

        try:
            with path.open(
                "x",
                encoding="utf-8",
            ) as file:
                file.write(
                    data
                )
                file.flush()
                os.fsync(
                    file.fileno()
                )

        except FileExistsError:
            existing = self.get(
                record.ingestion_event_id
            )

            if existing == record:
                return

            raise ProcessingExecutionConflictError(
                "Conflicting processing "
                "execution record for "
                f"{record.ingestion_event_id}"
            ) from None

        except OSError as exc:
            raise ProcessingExecutionRecordError(
                "Unable to write processing "
                f"execution record: {path}"
            ) from exc

    def _path(
        self,
        ingestion_event_id: UUID,
    ) -> Path:
        return (
            self.events_root
            / "idempotency"
            / "processing"
            / f"{ingestion_event_id}.json"
        )


def _required_string(
    document: dict[object, object],
    key: str,
) -> str:
    value = document.get(
        key
    )

    if not isinstance(
        value,
        str,
    ):
        raise ValueError(
            f"{key} must be a string"
        )

    if not value.strip():
        raise ValueError(
            f"{key} must not be blank"
        )

    return value


def _optional_string(
    document: dict[object, object],
    key: str,
) -> str | None:
    value = document.get(
        key
    )

    if value is None:
        return None

    if not isinstance(
        value,
        str,
    ):
        raise ValueError(
            f"{key} must be a string or null"
        )

    return _clean_optional_text(
        value
    )


def _clean_optional_text(
    value: str | None,
) -> str | None:
    if value is None:
        return None

    cleaned = value.strip()

    if not cleaned:
        raise ValueError(
            "Optional path must not be blank"
        )

    return cleaned


def _parse_datetime(
    value: str,
) -> datetime:
    return datetime.fromisoformat(
        value.replace(
            "Z",
            "+00:00",
        )
    )


def _format_datetime(
    value: datetime,
) -> str:
    return (
        value.astimezone(
            UTC
        )
        .isoformat()
        .replace(
            "+00:00",
            "Z",
        )
    )